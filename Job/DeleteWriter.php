<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;

class DeleteWriter extends AbstractJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		if (!$this->configuration->bucketId) {
			throw new WrongConfigurationException('Writer has been already deleted.');
		}

		$this->restApi->login($this->mainConfig['gd']['username'], $this->mainConfig['gd']['password']);

		foreach ($this->sharedConfig->getProjects($job['projectId'], $job['writerId']) as $project) {

			$this->sharedConfig->enqueueProjectToDelete($job['projectId'], $job['writerId'], $project['pid']);

			// Disable users in project
			$projectUsers = $this->restApi->get(sprintf('/gdc/projects/%s/users', $project['pid']));
			foreach ($projectUsers['users'] as $user) {
				if ($user['user']['content']['email'] != $this->mainConfig['gd']['username']) {
					$this->restApi->disableUserInProject($user['user']['links']['self'], $project['pid']);
				}
			}
		}

		// Remove only users created by Writer
		$writerDomain = substr($this->mainConfig['gd']['user_email'], strpos($this->mainConfig['gd']['user_email'], '@'));
		foreach ($this->sharedConfig->getUsers($job['projectId'], $job['writerId']) as $user) {
			if (strpos($user['email'], $writerDomain) !== false) {
				$this->sharedConfig->enqueueUserToDelete($job['projectId'], $job['writerId'], $user['uid'], $user['email']);
			}
		}

		$this->configuration->deleteWriter();

		return array();
	}
}