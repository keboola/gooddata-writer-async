<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\GoodData\UserAlreadyExistsException;
use Keboola\GoodDataWriter\Exception\JobProcessException;
use Keboola\GoodDataWriter\Writer\SharedConfig;

class CreateWriter extends AbstractJob
{
	/**
	 * required: (accessToken, projectName) || (pid, username, password)
	 * optional:
	 */
	public function run($job, $params)
	{
		try {
			$this->configuration->updateDataSetsFromSapi();

			$gdWriteStartTime = date('c');
			$username = sprintf($this->appConfiguration->gd_userEmailTemplate, $job['projectId'], $job['writerId'] . '-' . uniqid());
			$password = md5(uniqid());

			$existingProject = !empty($params['pid']) && !empty($params['username']) && !empty($params['password']);

			// Check setup for existing project
			if ($existingProject) {
				try {
					$this->restApi->login($params['username'], $params['password']);
				} catch (\Exception $e) {
					throw new JobProcessException($this->translator->trans('parameters.gd.credentials'));
				}
				if (!$this->restApi->hasAccessToProject($params['pid'])) {
					throw new JobProcessException($this->translator->trans('parameters.gd.project_inaccessible'));
				}
				if (!in_array('admin', $this->restApi->getUserRolesInProject($params['username'], $params['pid']))) {
					throw new JobProcessException($this->translator->trans('parameters.gd.user_not_admin'));
				}
			} else {
				$this->checkParams($params, array('accessToken', 'projectName'));
			}

			// Create writer's GD user
			$this->restApi->login($this->domainUser->username, $this->domainUser->password);
			try {
				$userId = $this->restApi->createUser($this->domainUser->domain, $username, $password, 'KBC', 'Writer', $this->appConfiguration->gd_ssoProvider);
			} catch (UserAlreadyExistsException $e) {
				$userId = $e->getMessage();
				if (!$userId) {
					throw new \Exception($this->translator->trans('error.user.in_other_domain') . ': ' . $username);
				}
			}

			// Create project or login via given credentials for adding user to project
			if ($existingProject) {
				$projectPid = $params['pid'];
				$this->restApi->login($params['username'], $params['password']);
				try {
					$this->restApi->inviteUserToProject($this->domainUser->username, $projectPid, RestApi::USER_ROLE_ADMIN);
				} catch (RestApiException $e) {
					$details = $e->getDetails();
					if ($e->getCode() != 400 || !isset($details['details']['error']['message']) || strpos($details['details']['error']['message'], 'already member') === false) {
						throw $e;
					}
				}

				$this->configuration->updateWriter('waitingForInvitation', '1');
				$this->configuration->updateWriter('maintenance', '1');

				$tokenData = $this->storageApiClient->getLogData();
				$waitJob = $this->sharedConfig->createJob($this->configuration->projectId, $this->configuration->writerId,
					$this->storageApiClient->getRunId(), $this->storageApiClient->token, $tokenData['id'], $tokenData['description'], array(
						'command' => 'waitForInvitation',
						'createdTime' => date('c'),
						'parameters' => array(
							'try' => 1
						),
						'queue' => SharedConfig::SERVICE_QUEUE
					));
				$this->queue->enqueue(array(
					'projectId' => $waitJob['projectId'],
					'writerId' => $waitJob['writerId'],
					'batchId' => $waitJob['batchId']
				), 30);

			} else {
				$projectPid = $this->restApi->createProject($params['projectName'], $params['accessToken'], json_encode(array(
					'projectId' => $this->configuration->projectId,
					'writerId' => $this->configuration->writerId,
					'main' => true
				)));
				$this->restApi->addUserToProject($userId, $projectPid, RestApi::USER_ROLE_ADMIN);
			}


			// Save data to configuration bucket
			$this->configuration->updateWriter('gd.pid', $projectPid);
			$this->configuration->updateWriter('gd.username', $username);
			$this->configuration->updateWriter('gd.password', $password, true);
			$this->configuration->updateWriter('gd.uid', $userId);


			$this->sharedConfig->saveProject($job['projectId'], $job['writerId'], $projectPid, isset($params['accessToken']) ? $params['accessToken'] : null, $existingProject);
			$this->sharedConfig->saveUser($job['projectId'], $job['writerId'], $userId, $username);
			$this->sharedConfig->setWriterStatus($job['projectId'], $job['writerId'], SharedConfig::WRITER_STATUS_READY);


			$this->logEvent('createUser', array(
				'duration' => time() - strtotime($gdWriteStartTime)
			), $this->restApi->getLogPath());
			return array(
				'uid' => $userId,
				'pid' => $projectPid,
				'gdWriteStartTime' => $gdWriteStartTime
			);
		} catch (\Exception $e) {
			$this->sharedConfig->setWriterStatus($job['projectId'], $job['writerId'], SharedConfig::WRITER_STATUS_ERROR);
			$this->configuration->updateWriter('failure', $e->getMessage());
			throw $e;
		}
	}
}