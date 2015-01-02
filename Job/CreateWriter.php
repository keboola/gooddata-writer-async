<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\GoodData\UserAlreadyExistsException;
use Keboola\GoodDataWriter\Exception\JobProcessException;
use Keboola\GoodDataWriter\Writer\SharedStorage;

class CreateWriter extends AbstractJob
{

	public function prepare($params, RestApi $restApi=null)
	{
		$tokenInfo = $this->storageApiClient->getLogData();
		$projectId = $tokenInfo['owner']['id'];

		$this->checkParams($params, array('writerId'));
		if (!preg_match('/^[a-zA-Z0-9_]+$/', $params['writerId'])) {
			throw new WrongParametersException($this->translator->trans('parameters.writerId.format'));
		}
		if (strlen($params['writerId'] > 50)) {
			throw new WrongParametersException($this->translator->trans('parameters.writerId.length'));
		}

		$result = array();
		if (!empty($params['description'])) {
			$result['description'] = $params['description'];
		}

		if (!empty($params['username']) || !empty($params['password']) || !empty($params['pid'])) {
			if (empty($params['username'])) {
				throw new WrongParametersException($this->translator->trans('parameters.username_missing'));
			}
			if (empty($params['password'])) {
				throw new WrongParametersException($this->translator->trans('parameters.password_missing'));
			}
			if (empty($params['pid'])) {
				throw new WrongParametersException($this->translator->trans('parameters.pid_missing'));
			}

			$result['pid'] = $params['pid'];
			$result['username'] = $params['username'];
			$result['password'] = $params['password'];


			try {
				$restApi->login($params['username'], $params['password']);
			} catch (\Exception $e) {
				throw new WrongParametersException($this->translator->trans('parameters.gd.credentials'));
			}
			if (!$restApi->hasAccessToProject($params['pid'])) {
				throw new WrongParametersException($this->translator->trans('parameters.gd.project_inaccessible'));
			}
			if (!in_array('admin', $restApi->getUserRolesInProject($params['username'], $params['pid']))) {
				throw new WrongParametersException($this->translator->trans('parameters.gd.user_not_admin'));
			}
		} else {
			$result['accessToken'] = !empty($params['accessToken'])? $params['accessToken'] : $this->gdAccessToken;
			$result['projectName'] = sprintf(Model::PROJECT_NAME_TEMPLATE, $this->gdProjectNamePrefix, $tokenInfo['owner']['name'], $params['writerId']);
		}

		if (isset($params['users'])) {
			$result['users'] = is_array($params['users']) ? $params['users'] : explode(',', $params['users']);
		}

		$this->configuration->createWriter($params['writerId']);
		$this->sharedStorage->setWriterStatus($projectId, $params['writerId'], SharedStorage::WRITER_STATUS_PREPARING);

		return $result;
	}

	/**
	 * required: (accessToken, projectName) || (pid, username, password)
	 * optional: description
	 */
	public function run($job, $params, RestApi $restApi)
	{
		try {
			$this->configuration->updateDataSetsFromSapi();

			$username = sprintf(Model::USERNAME_TEMPLATE, $job['projectId'], $job['writerId'] . '-' . uniqid(), $this->gdUsernameDomain);
			$password = md5(uniqid());

			$existingProject = !empty($params['pid']) && !empty($params['username']) && !empty($params['password']);

			// Check setup for existing project
			if ($existingProject) {
				try {
					$restApi->login($params['username'], $params['password']);
				} catch (\Exception $e) {
					throw new JobProcessException($this->translator->trans('parameters.gd.credentials'));
				}
				if (!$restApi->hasAccessToProject($params['pid'])) {
					throw new JobProcessException($this->translator->trans('parameters.gd.project_inaccessible'));
				}
				if (!in_array('admin', $restApi->getUserRolesInProject($params['username'], $params['pid']))) {
					throw new JobProcessException($this->translator->trans('parameters.gd.user_not_admin'));
				}
			} else {
				$this->checkParams($params, array('accessToken', 'projectName'));
			}

			// Create writer's GD user
			$restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);
			try {
				$userId = $restApi->createUser($this->getDomainUser()->domain, $username, $password, 'KBC', 'Writer', $this->gdSsoProvider);
			} catch (UserAlreadyExistsException $e) {
				$userId = $e->getMessage();
				if (!$userId) {
					throw new \Exception($this->translator->trans('error.user.in_other_domain') . ': ' . $username);
				}
			}

			// Create project or login via given credentials for adding user to project
			if ($existingProject) {
				$projectPid = $params['pid'];
				$restApi->login($params['username'], $params['password']);
				try {
					$restApi->inviteUserToProject($this->getDomainUser()->username, $projectPid, RestApi::USER_ROLE_ADMIN);
				} catch (RestApiException $e) {
					$details = $e->getDetails();
					if ($e->getCode() != 400 || !isset($details['details']['error']['message']) || strpos($details['details']['error']['message'], 'already member') === false) {
						throw $e;
					}
				}

				$this->configuration->updateWriter('waitingForInvitation', '1');
				$this->sharedStorage->setWriterStatus($job['projectId'], $job['writerId'], SharedStorage::WRITER_STATUS_MAINTENANCE);

				$tokenData = $this->storageApiClient->getLogData();
				$waitJob = $this->sharedStorage->createJob($this->storageApiClient->generateId(),
					$this->configuration->projectId, $this->configuration->writerId, array(
						'command' => 'waitForInvitation',
						'createdTime' => date('c'),
						'parameters' => array(
							'try' => 1
						),
						'runId' => $this->storageApiClient->getRunId(),
						'token' => $this->storageApiClient->token,
						'tokenId' => $tokenData['id'],
						'tokenDesc' => $tokenData['description']
					), SharedStorage::SERVICE_QUEUE);
				$this->queue->enqueue(array(
					'projectId' => $waitJob['projectId'],
					'writerId' => $waitJob['writerId'],
					'batchId' => $waitJob['batchId']
				), 30);

			} else {
				$projectPid = $restApi->createProject($params['projectName'], $params['accessToken'], json_encode(array(
					'projectId' => $this->configuration->projectId,
					'writerId' => $this->configuration->writerId,
					'main' => true
				)));
				$restApi->addUserToProject($userId, $projectPid, RestApi::USER_ROLE_ADMIN);
			}


			// Save data to configuration bucket
			$this->configuration->updateWriter('gd.pid', $projectPid);
			$this->configuration->updateWriter('gd.username', $username);
			$this->configuration->updateWriter('gd.password', $password, true);
			$this->configuration->updateWriter('gd.uid', $userId);

			if (!empty($params['description'])) {
				$this->configuration->updateWriter('description', $params['description']);
			}


			$this->sharedStorage->saveProject($job['projectId'], $job['writerId'], $projectPid, isset($params['accessToken']) ? $params['accessToken'] : null, $existingProject);
			$this->sharedStorage->saveUser($job['projectId'], $job['writerId'], $userId, $username);
			$this->sharedStorage->setWriterStatus($job['projectId'], $job['writerId'], SharedStorage::WRITER_STATUS_READY);


			return array(
				'uid' => $userId,
				'pid' => $projectPid
			);
		} catch (\Exception $e) {
			$this->sharedStorage->updateWriter($job['projectId'], $job['writerId'], array(
				'status' => SharedStorage::WRITER_STATUS_ERROR,
				'failure' => $e->getMessage()
			));
			throw $e;
		}
	}
}