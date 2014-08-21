<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 25.02.14
 * Time: 17:23
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\RestApiException;

class ResetProject extends AbstractJob
{

	public function prepare($params)
	{
		$this->checkParams($params, array('writerId'));
		$this->checkWriterExistence($params['writerId']);

		return array(
			'removeClones' => isset($params['removeClones'])? (bool)$params['removeClones'] : false
		);
	}

	/**
	 * required:
	 * optional: removeClones, accessToken
	 */
	function run($job, $params, RestApi $restApi)
	{
		$gdWriteStartTime = date('c');
		$removeClones = isset($params['removeClones']) ? (bool)$params['removeClones'] : false;

		$projectName = sprintf($this->appConfiguration->gd_projectNameTemplate, $this->configuration->tokenInfo['owner']['name'],
			$this->configuration->writerId);
		$accessToken = !empty($params['accessToken']) ? $params['accessToken'] : $this->appConfiguration->gd_accessToken;
		$bucketAttributes = $this->configuration->bucketAttributes();

		$oldPid = $bucketAttributes['gd']['pid'];
		$userId = $bucketAttributes['gd']['uid'];

		$restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);
		$newPid = $restApi->createProject($projectName, $accessToken, json_encode(array(
			'projectId' => $this->configuration->projectId,
			'writerId' => $this->configuration->writerId,
			'main' => true
		)));

		// All users from old project add to the new one with the same role
		$oldRoles = array();
		foreach($restApi->usersInProject($oldPid) as $user) {
			$userEmail = $user['user']['content']['email'];
			if ($userEmail == $this->getDomainUser()->username) continue;

			$userId = RestApi::getUserId($user['user']['links']['self']);
			if (isset($user['user']['content']['userRoles'])) foreach ($user['user']['content']['userRoles'] as $roleUri) {
				if (!in_array($roleUri, array_keys($oldRoles))) {
					$role = $restApi->get($roleUri);
					if (isset($role['projectRole']['meta']['identifier']))
						$oldRoles[$roleUri] = $role['projectRole']['meta']['identifier'];
				}
				if (isset($oldRoles[$roleUri])) {
					try {
						$restApi->addUserToProject($userId, $newPid, $oldRoles[$roleUri]);
					} catch (RestApiException $e) {
						$restApi->inviteUserToProject($userEmail, $newPid, $oldRoles[$roleUri]);
					}
				}
			}

			$restApi->disableUserInProject($user['user']['links']['self'], $oldPid);
		}

		$this->sharedConfig->enqueueProjectToDelete($job['projectId'], $job['writerId'], $oldPid);

		if ($removeClones) {
			foreach ($this->configuration->getProjects() as $p) {
				if (empty($p['main'])) {
					$restApi->disableUserInProject(RestApi::getUserUri($userId), $p['pid']);
					$this->sharedConfig->enqueueProjectToDelete($job['projectId'], $job['writerId'], $p['pid']);
				}
			}
			$this->configuration->resetProjectsTable();
		}

		$this->configuration->updateWriter('gd.pid', $newPid);

		foreach ($this->configuration->getDataSets() as $dataSet) {
			$this->configuration->updateDataSetDefinition($dataSet['id'], array(
				'isExported' => null
			));
		}
		foreach ($this->configuration->getDateDimensions() as $dimension) {
			$this->configuration->setDateDimensionIsNotExported($dimension['name']);
		}

		$this->logEvent('ResetProject', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $restApi->getLogPath());
		return array(
			'newPid' => $newPid,
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}