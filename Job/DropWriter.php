<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException;

class DropWriter extends AbstractJob
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

		if ($dropImmediately) {
			$this->restApi->setCredentials($mainConfig['username'], $mainConfig['password']);
			//@TODO
		}

		$pids = array();
		foreach ($this->sharedConfig->getProjects($job['projectId'], $job['writerId']) as $project) {
			$this->sharedConfig->enqueueProjectToDelete($job['projectId'], $job['writerId'], $project['pid'], $project['backendUrl'], empty($params['dev']));

			if ($dropImmediately) {
				try {
					$this->restApi->dropProject($project['pid']);
					$pids[] = $project['pid'];
				} catch (RestApiException $e) {
					$this->log->alert('Could nor delete project', array(
						'project' => $project,
						'exception' => $e
					));
				}
			}
		}
		if (count($pids)) {
			$this->sharedConfig->markProjectsDeleted($pids);
		}

		$uids = array();
		foreach ($this->sharedConfig->getUsers($job['projectId'], $job['writerId']) as $user) {
			$this->sharedConfig->enqueueUserToDelete($job['projectId'], $job['writerId'], $user['uid'], $user['email'], empty($params['dev']));

			if ($dropImmediately) {
				try {
					$this->restApi->dropUser($user['uid']);
					$uids[] = $user['uid'];
				} catch (RestApiException $e) {
					$this->log->alert('Could nor delete user', array(
						'user' => $user,
						'exception' => $e
					));
				}
			}
		}
		if (count($uids)) {
			$this->sharedConfig->markUsersDeleted($uids);
		}

		$this->configuration->dropBucket();

		return $this->_prepareResult($job['id'], array(), null);
	}
}