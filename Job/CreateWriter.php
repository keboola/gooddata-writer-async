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
				throw new JobProcessException('Given GoodData credentials does not work');
			}
			try {
				if (!in_array('admin', $this->restApi->getUserRolesInProject($params['username'], $params['pid']))) {
					throw new JobProcessException('Given GoodData credentials must have admin access to the project');
				}
			} catch (RestApiException $e) {
				throw new JobProcessException('GoodData project is not accessible under given credentials');
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
				throw new \Exception(sprintf("User '%s' already exists and does not belong to domain '%s'", $username, $this->domainUser->domain));
			}
		}

		// Create project or login via given credentials for adding user to project
		if ($existingProject) {
			$projectPid = $params['pid'];
			$this->restApi->login($params['username'], $params['password']);
			$this->restApi->inviteUserToProject($this->domainUser->username, $projectPid, RestApi::USER_ROLE_ADMIN);

			$this->configuration->updateWriter('waitingForInvitation', '1');
			$this->configuration->updateWriter('maintenance', '1');

			$waitJob = $this->sharedConfig->createJob($this->configuration->projectId, $this->configuration->writerId, $this->storageApiClient, array(
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


		$this->sharedConfig->saveProject($job['projectId'], $job['writerId'], $projectPid, isset($params['accessToken'])? $params['accessToken'] : null, $existingProject);
		$this->sharedConfig->saveUser($userId, $username, $job);


		$this->logEvent('createUser', array(
			'duration' => time() - strtotime($gdWriteStartTime)
		), $this->restApi->getLogPath());
		return array(
			'uid' => $userId,
			'pid' => $projectPid,
			'gdWriteStartTime' => $gdWriteStartTime
		);
	}
}