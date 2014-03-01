<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 25.02.14
 * Time: 17:23
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\GoodData\RestApi;

class ResetProject extends AbstractJob
{
	function run($job, $params)
	{
		$gdWriteStartTime = date('c');
		$removeClones = isset($params['removeClones']) ? (bool)$params['removeClones'] : false;

		$projectName = sprintf($this->appConfiguration->gd_projectNameTemplate, $this->configuration->tokenInfo['owner']['name'],
			$this->configuration->writerId);
		$accessToken = !empty($params['accessToken']) ? $params['accessToken'] : $this->appConfiguration->gd_accessToken;
		$bucketAttributes = $this->configuration->bucketAttributes();

		$oldPid = $bucketAttributes['gd']['pid'];
		$userId = $bucketAttributes['gd']['uid'];

		$this->restApi->login($this->appConfiguration->gd_username, $this->appConfiguration->gd_password);
		$newPid = $this->restApi->createProject($projectName, $accessToken);

		// All users from old project add to the new one with the same role
		$oldRoles = array();
		foreach($this->restApi->usersInProject($oldPid) as $user) {
			if ($user['user']['content']['email'] == $this->appConfiguration->gd_username) continue;

			$userId = RestApi::getUserId($user['user']['links']['self']);
			if (isset($user['user']['content']['userRoles'])) foreach ($user['user']['content']['userRoles'] as $roleUri) {
				if (!in_array($roleUri, array_keys($oldRoles))) {
					$role = $this->restApi->get($roleUri);
					if (isset($role['projectRole']['meta']['identifier']))
						$oldRoles[$roleUri] = $role['projectRole']['meta']['identifier'];
				}
				if (isset($oldRoles[$roleUri])) {
					$this->restApi->addUserToProject($userId, $newPid, $oldRoles[$roleUri]);
				}
			}
			$this->restApi->disableUserInProject($user['user']['links']['self'], $oldPid);
		}

		$this->sharedConfig->enqueueProjectToDelete($job['projectId'], $job['writerId'], $oldPid);

		if ($removeClones) {
			foreach ($this->configuration->getProjects() as $p) {
				if (empty($p['main'])) {
					$this->restApi->disableUserInProject(RestApi::getUserUri($userId), $p['pid']);
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
		), $this->restApi->getLogPath());
		return array(
			'newPid' => $newPid,
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}