<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 25.02.14
 * Time: 17:23
 */

namespace Keboola\GoodDataWriter\Job;

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
		$this->restApi->addUserToProject($userId, $newPid, 'adminRole');


		$this->restApi->disableUserInProject($userId, $oldPid);
		$this->sharedConfig->enqueueProjectToDelete($job['projectId'], $job['writerId'], $oldPid);

		if ($removeClones) {
			foreach ($this->configuration->getProjects() as $p) {
				if (empty($p['main'])) {
					$this->restApi->disableUserInProject($userId, $p['pid']);
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