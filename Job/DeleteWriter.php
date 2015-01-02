<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\RestApiException;

class DeleteWriter extends AbstractJob
{

	public function prepare($params)
	{
		$this->checkParams($params, array('writerId'));
		$this->checkWriterExistence($params['writerId']);

		$this->sharedStorage->cancelJobs($this->configuration->projectId, $this->configuration->writerId);
		$this->sharedStorage->deleteWriter($this->configuration->projectId, $this->configuration->writerId);

		return array();
	}

	/**
	 * required:
	 * optional:
	 */
	public function run($job, $params, RestApi $restApi)
	{
		if (!$this->configuration->bucketId) {
			throw new WrongConfigurationException('Writer has been already deleted.');
		}

		$restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);

		foreach ($this->sharedStorage->getProjects($job['projectId'], $job['writerId']) as $project) if (!$project['keep_on_removal']) {
			if ($this->configuration->testingWriter) {
				try {
					$restApi->dropProject($project['pid']);
				} catch (RestApiException $e) {
					// Ignore, project may have been already deleted
				}
				$this->sharedStorage->markProjectsDeleted(array($project['pid']));
			} else {
				$this->sharedStorage->enqueueProjectToDelete($job['projectId'], $job['writerId'], $project['pid']);

				// Disable users in project
				try {
					$projectUsers = $restApi->get(sprintf('/gdc/projects/%s/users', $project['pid']));
					foreach ($projectUsers['users'] as $user) {
						if ($user['user']['content']['email'] != $this->getDomainUser()->username) {
							$restApi->disableUserInProject($user['user']['links']['self'], $project['pid']);
						}
					}
				} catch (RestApiException $e) {
					// Ignore, project may have been already deleted
				}
			}
		}

		// Remove only users created by Writer
		foreach ($this->sharedStorage->getUsers($job['projectId'], $job['writerId']) as $user) {
			if (strpos($user['email'], $this->gdUsernameDomain) !== false) {
				if ($this->configuration->testingWriter) {
					$restApi->dropUser($user['uid']);
					$this->sharedStorage->markUsersDeleted(array($user['uid']));
				} else {
					$this->sharedStorage->enqueueUserToDelete($job['projectId'], $job['writerId'], $user['uid']);
				}
			}
		}

		$this->configuration->deleteWriter();

		return array();
	}
}