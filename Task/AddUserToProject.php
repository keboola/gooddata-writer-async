<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\Syrup\Exception\UserException;

class AddUserToProject extends AbstractTask
{

    public function prepare($params)
    {
        $this->checkParams($params, ['writerId', 'pid', 'email', 'role']);
        $this->checkWriterExistence($params['writerId']);

        $allowedRoles = array_keys(RestApi::$userRoles);
        if (!in_array($params['role'], $allowedRoles)) {
            throw new UserException($this->translator->trans('parameters.role %1', ['%1' => implode(', ', $allowedRoles)]));
        }
        if (!$this->configuration->getProject($params['pid'])) {
            throw new UserException($this->translator->trans('parameters.pid_not_configured'));
        }

        $bucketAttributes = $this->configuration->getBucketAttributes();
        $this->configuration->checkProjectsTable();
        $this->configuration->checkUsersTable();
        $this->configuration->checkProjectUsersTable();

        if (empty($params['pid'])) {
            $params['pid'] = $bucketAttributes['gd']['pid'];
        }
        return [
            'email' => $params['email'],
            'pid' => $params['pid'],
            'role' => $params['role'],
            'createUser' => isset($params['createUser']) ? 1 : null
        ];
    }

    /**
     * required: pid, email, role
     * optional: createUser
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);

        $this->checkParams($params, ['email', 'role']);
        $params['email'] = strtolower($params['email']);

        if (empty($params['pid'])) {
            $bucketAttributes = $this->configuration->getBucketAttributes();
            $params['pid'] = $bucketAttributes['gd']['pid'];
        }

        $allowedRoles = array_keys(RestApi::$userRoles);
        if (!in_array($params['role'], $allowedRoles)) {
            throw new UserException($this->translator->trans('parameters.role %1', ['%1' => implode(', ', $allowedRoles)]));
        }

        $this->restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);

        $userId = null;

        // Get user uri
        $user = $this->configuration->getUser($params['email']);

        if ($user && $user['uid']) {
            // user created by writer
            $userId = $user['uid'];
        } else {
            $userId = $this->restApi->userId($params['email'], $this->getDomainUser()->domain);
            if ($userId) {
                // user in domain
                $this->configuration->saveUser($params['email'], $userId);
            }
        }

        if (!$userId) {
            if (!empty($params['createUser'])) {
                // try create new user in domain
                $childTask = $this->taskFactory->create('createUser');
                $childParams = [
                    'email' => $params['email'],
                    'firstName' => 'KBC',
                    'lastName' => $params['email'],
                    'password' => md5(uniqid() . str_repeat($params['email'], 2)),
                ];
                $result = $childTask->run($job, $taskId+1, $childParams);
                if (!empty($result['uid'])) {
                    $userId = $result['uid'];
                }

                // add task to job metadata
                $job->addTask('createUser', $childParams);
                $this->jobFactory->update($job);
            }
        }

        if ($userId) {
            $this->restApi->addUserToProject($userId, $params['pid'], RestApi::$userRoles[$params['role']]);

            $this->configuration->saveProjectUser($params['pid'], $params['email'], $params['role'], false);
        } else {
            $this->restApi->inviteUserToProject($params['email'], $params['pid'], RestApi::$userRoles[$params['role']]);

            $this->configuration->saveProjectUser($params['pid'], $params['email'], $params['role'], true);
        }

        $result = [];
        if ($userId) {
            $result['flags'] = ['invitation' => $this->translator->trans('result.flag.invitation')];
        }

        return $result;
    }
}
