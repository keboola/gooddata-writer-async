<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\Exception\RestApiException;
use Keboola\GoodDataWriter\Exception\UserAlreadyExistsException;
use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\GoodDataWriter\Writer\SharedStorage;
use Keboola\Syrup\Exception\UserException;

class CreateWriter extends AbstractTask
{

    public function prepare($params)
    {
        $tokenInfo = $this->storageApiClient->getLogData();
        $projectId = $tokenInfo['owner']['id'];

        $this->checkParams($params, ['writerId']);
        if (!preg_match('/^[a-zA-Z0-9_]+$/', $params['writerId'])) {
            throw new UserException($this->translator->trans('parameters.writerId.format'));
        }
        if (strlen($params['writerId']) > 50) {
            throw new UserException($this->translator->trans('parameters.writerId.length'));
        }

        $result = [];
        if (!empty($params['description'])) {
            $result['description'] = $params['description'];
        }

        if (!empty($params['username']) || !empty($params['password']) || !empty($params['pid'])) {
            if (empty($params['username'])) {
                throw new UserException($this->translator->trans('parameters.username_missing'));
            }
            if (empty($params['password'])) {
                throw new UserException($this->translator->trans('parameters.password_missing'));
            }
            if (empty($params['pid'])) {
                throw new UserException($this->translator->trans('parameters.pid_missing'));
            }

            $result['pid'] = $params['pid'];
            $result['username'] = $params['username'];
            $result['password'] = $params['password'];

            $this->checkExistingProject($params);
        } else {
            $result['accessToken'] = !empty($params['accessToken'])? $params['accessToken'] : $this->gdAccessToken;
            $result['projectName'] = sprintf(Model::PROJECT_NAME_TEMPLATE, $this->gdProjectNamePrefix, $tokenInfo['owner']['name'], $params['writerId']);
        }

        if (isset($params['users'])) {
            $result['users'] = is_array($params['users']) ? $params['users'] : explode(',', $params['users']);
        }

        $this->configuration->createBucket($params['writerId']);
        $this->sharedStorage->setWriterStatus($projectId, $params['writerId'], SharedStorage::WRITER_STATUS_PREPARING);

        return $result;
    }

    /**
     * required: (accessToken, projectName) || (pid, username, password)
     * optional: description
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        try {
            $this->configuration->updateDataSetsFromSapi();

            $username = sprintf(Model::USERNAME_TEMPLATE, $this->configuration->projectId, $this->configuration->writerId . '-' . uniqid(), $this->gdUsernameDomain);
            $password = md5(uniqid());

            $existingProject = !empty($params['pid']) && !empty($params['username']) && !empty($params['password']);

            // Check setup for existing project
            if ($existingProject) {
                $this->checkExistingProject($params);
            } else {
                $this->checkParams($params, ['accessToken', 'projectName']);
            }

            // Create writer's GD user
            $this->restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);
            try {
                $userId = $this->restApi->createUser($this->getDomainUser()->domain, $username, $password, 'KBC', 'Writer', $this->gdSsoProvider);
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
                    $this->restApi->inviteUserToProject($this->getDomainUser()->username, $projectPid, RestApi::USER_ROLE_ADMIN);
                } catch (RestApiException $e) {
                    $details = $e->getData();
                    if ($e->getCode() != 400 || !isset($details['details']['error']['message']) || strpos($details['details']['error']['message'], 'already member') === false) {
                        throw $e;
                    }
                }

                $this->configuration->updateBucketAttribute('waitingForInvitation', '1');
                $this->sharedStorage->setWriterStatus($this->configuration->projectId, $this->configuration->writerId, SharedStorage::WRITER_STATUS_MAINTENANCE);

                $job = $this->jobFactory->create(Job::SERVICE_QUEUE);
                $job->addTask('waitForInvitation', ['try' => 1]);
                $this->jobFactory->enqueue($job, 30);

            } else {
                $projectPid = $this->restApi->createProject($params['projectName'], $params['accessToken'], json_encode([
                    'projectId' => $this->configuration->projectId,
                    'writerId' => $this->configuration->writerId,
                    'main' => true
                ]));
                $this->restApi->addUserToProject($userId, $projectPid, RestApi::USER_ROLE_ADMIN);
            }


            // Save data to configuration bucket
            $this->configuration->updateBucketAttribute('gd.pid', $projectPid);
            $this->configuration->updateBucketAttribute('gd.username', $username);
            $this->configuration->updateBucketAttribute('gd.password', $password, true);
            $this->configuration->updateBucketAttribute('gd.uid', $userId);

            if (!empty($params['description'])) {
                $this->configuration->updateBucketAttribute('description', $params['description']);
            }


            $this->sharedStorage->saveProject($this->configuration->projectId, $this->configuration->writerId, $projectPid, isset($params['accessToken']) ? $params['accessToken'] : null, $existingProject);
            $this->sharedStorage->saveUser($this->configuration->projectId, $this->configuration->writerId, $userId, $username);
            $this->sharedStorage->setWriterStatus($this->configuration->projectId, $this->configuration->writerId, SharedStorage::WRITER_STATUS_READY);


            return [
                'uid' => $userId,
                'pid' => $projectPid
            ];
        } catch (\Exception $e) {
            $this->sharedStorage->updateWriter($this->configuration->projectId, $this->configuration->writerId, [
                'status' => SharedStorage::WRITER_STATUS_ERROR,
                'failure' => $e->getMessage()
            ]);
            throw $e;
        }
    }

    private function checkExistingProject($params)
    {
        try {
            $this->restApi->login($params['username'], $params['password']);
        } catch (\Exception $e) {
            throw new UserException($this->translator->trans('parameters.gd.credentials'));
        }
        if (!$this->restApi->hasAccessToProject($params['pid'])) {
            throw new UserException($this->translator->trans('parameters.gd.project_inaccessible'));
        }
        if (!in_array('admin', $this->restApi->getUserRolesInProject($params['username'], $params['pid']))) {
            throw new UserException($this->translator->trans('parameters.gd.user_not_admin'));
        }
    }
}
