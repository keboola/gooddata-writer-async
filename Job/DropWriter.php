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
		$dropImmediately = !empty($params['immediately']);

		if (!$this->configuration->bucketId) {
			throw new WrongConfigurationException('Writer has been already deleted.');
		}

		foreach ($this->configuration->getProjects() as $project) {
			if ($dropImmediately) {

			} else {
				$this->sharedConfig->enqueueProjectToDelete($job['projectId'], $job['writerId'], $project['pid'], empty($params['dev']));
			}
		}
		foreach ($this->configuration->getUsers() as $user) {
			if (!$user['uid']) {
				$user['uid'] = $this->restApi->userId($user['email'], $mainConfig['domain']);
			}
			if ($dropImmediately) {

			} else {
				$this->sharedConfig->enqueueUserToDelete($job['projectId'], $job['writerId'], $user['uid'], $user['email'], empty($params['dev']));
			}
		}

		$this->configuration->dropBucket();

		return $this->_prepareResult($job['id'], array(), null);
	}
}