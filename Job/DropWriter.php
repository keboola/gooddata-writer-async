<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;

class DropWriter extends GenericJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		$env = empty($params['dev']) ? 'prod' :'dev';
		$mainConfig = $this->mainConfig['gd'][$env];

		foreach ($this->configuration->getProjects() as $project) {
			$this->sharedConfig->enqueueProjectToDelete($job['projectId'], $job['writerId'], $project['pid'], empty($params['dev']));
		}
		foreach ($this->configuration->getUsers() as $user) {
			if (!$user['uri']) {
				$user['uri'] = $this->restApi->userUri($user['email'], $mainConfig['domain']);
			}
			$this->sharedConfig->enqueueUserToDelete($job['projectId'], $job['writerId'], $user['uri'], $user['email'], empty($params['dev']));
		}

		$this->configuration->dropBucket();

		return $this->_prepareResult($job['id'], array(), null);
	}
}