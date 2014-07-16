<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\GoodData\RestApiException;

class DeleteWriter extends AbstractJob
{
	/**
	 * required:
	 * optional:
	 */
	public function run($job, $params)
	{
		if (!$this->configuration->bucketId) {
			throw new WrongConfigurationException('Writer has been already deleted.');
		}

		$this->restApi->login($this->domainUser->username, $this->domainUser->password);

		foreach ($this->sharedConfig->getProjects($job['projectId'], $job['writerId']) as $project) if (!$project['keep_on_removal']) {
			if ($this->isTesting) {
				try {
					$this->restApi->dropProject($project['pid']);
				} catch (RestApiException $e) {
					// Ignore, project may have been already deleted
				}
				$this->sharedConfig->markProjectsDeleted($project['pid']);
			} else {
				$this->sharedConfig->enqueueProjectToDelete($job['projectId'], $job['writerId'], $project['pid']);

				// Disable users in project
				try {
					$projectUsers = $this->restApi->get(sprintf('/gdc/projects/%s/users', $project['pid']));
					foreach ($projectUsers['users'] as $user) {
						if ($user['user']['content']['email'] != $this->domainUser->username) {
							$this->restApi->disableUserInProject($user['user']['links']['self'], $project['pid']);
						}
					}
				} catch (RestApiException $e) {
					// Ignore, project may have been already deleted
				}
			}
		}

		// Remove only users created by Writer
		$writerDomain = substr($this->appConfiguration->gd_userEmailTemplate, strpos($this->appConfiguration->gd_userEmailTemplate, '@'));
		foreach ($this->sharedConfig->getUsers($job['projectId'], $job['writerId']) as $user) {
			if (strpos($user['email'], $writerDomain) !== false) {
				if ($this->isTesting) {
					$this->restApi->dropUser($user['uid']);
					$this->sharedConfig->markUsersDeleted($user['uid']);
				} else {
					$this->sharedConfig->enqueueUserToDelete($job['projectId'], $job['writerId'], $user['uid']);
				}
			}
		}

		$this->configuration->deleteWriter();
		$this->logEvent('deleteFilter', array(), $this->restApi->getLogPath());
		return array();
	}
}