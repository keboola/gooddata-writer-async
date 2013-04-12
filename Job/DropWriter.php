<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\JobRunException;

class DropWriter extends GenericJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws JobRunException
	 * @return array
	 */
	public function run($job, $params)
	{
		$env = empty($params['dev']) ? 'prod' :'dev';
		$mainConfig = $this->mainConfig['gd'][$env];

		$this->configuration->prepareProjects();
		$this->configuration->prepareUsers();

		$firstLine = true;
		foreach ($this->configuration->projectsCsv as $project) {
			if (!$firstLine)
				$this->sharedConfig->enqueueProjectToDelete($job['projectId'], $job['writerId'], $project[0], empty($params['dev']));
			$firstLine = false;
		}
		$firstLine = true;
		foreach ($this->configuration->usersCsv as $user) {
			if (!$firstLine)
				$this->sharedConfig->enqueueUserToDelete($job['projectId'], $job['writerId'], $user[1], $user[0], empty($params['dev']));
			$firstLine = false;
		}

		if (isset($this->configuration->bucketInfo['gd']['pid'])) {
			$this->sharedConfig->enqueueProjectToDelete($job['projectId'], $job['writerId'], $this->configuration->bucketInfo['gd']['pid'], empty($params['dev']));
		}

		if (isset($this->configuration->bucketInfo['gd']['username']) && !isset($this->configuration->bucketInfo['gd']['userUri'])) {
			$this->configuration->bucketInfo['gd']['userUri'] = $this->restApi->userUri($this->configuration->bucketInfo['gd']['username'], $mainConfig['domain']);
		}
		if (isset($this->configuration->bucketInfo['gd']['userUri'])) {
			$this->sharedConfig->enqueueUserToDelete($job['projectId'], $job['writerId'], $this->configuration->bucketInfo['gd']['userUri'],
				$this->configuration->bucketInfo['gd']['username'], empty($params['dev']));
		}

		$this->configuration->dropBucket();

		return $this->_prepareResult($job['id'], array(), null);
	}
}