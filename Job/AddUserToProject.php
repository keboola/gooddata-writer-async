<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\GoodData\RestApi;

class AddUserToProject extends AbstractJob
{

    public function prepare($params)
    {
        $this->checkParams($params, array('writerId', 'pid', 'email', 'role'));
        $this->checkWriterExistence($params['writerId']);

        $allowedRoles = array_keys(RestApi::$userRoles);
        if (!in_array($params['role'], $allowedRoles)) {
            throw new WrongParametersException($this->translator->trans('parameters.role %1', array('%1' => implode(', ', $allowedRoles))));
        }
        if (!$this->configuration->getProject($params['pid'])) {
            throw new WrongParametersException($this->translator->trans('parameters.pid_not_configured'));
        }

        $bucketAttributes = $this->configuration->bucketAttributes();
        $this->configuration->checkProjectsTable();
        $this->configuration->checkUsersTable();
        $this->configuration->checkProjectUsersTable();

        if (empty($params['pid'])) {
            $params['pid'] = $bucketAttributes['gd']['pid'];
        }
        return array(
            'email' => $params['email'],
            'pid' => $params['pid'],
            'role' => $params['role'],
            'createUser' => isset($params['createUser']) ? 1 : null
        );
    }

    /**
     * required: pid, email, role
     * optional: createUser
     */
    public function run($job, $params, RestApi $restApi)
    {
        $this->checkParams($params, array('email', 'role'));
        $params['email'] = strtolower($params['email']);

        if (empty($params['pid'])) {
            $bucketAttributes = $this->configuration->bucketAttributes();
            $params['pid'] = $bucketAttributes['gd']['pid'];
        }

        $allowedRoles = array_keys(RestApi::$userRoles);
        if (!in_array($params['role'], $allowedRoles)) {
            throw new WrongConfigurationException($this->translator->trans('role %1', array('%1' => implode(', ', $allowedRoles))));
        }

        $restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);

        $userId = null;

        // Get user uri
        $user = $this->configuration->getUser($params['email']);

        if ($user && $user['uid']) {
            // user created by writer
            $userId = $user['uid'];
        } else {
            $userId = $restApi->userId($params['email'], $this->getDomainUser()->domain);
            if ($userId) {
                // user in domain
                $this->configuration->saveUser($params['email'], $userId);
            }
        }

        if (!$userId) {
            if (!empty($params['createUser'])) {
                // try create new user in domain
                $childJob = $this->factory->getJobClass('createUser');

                $childParams = array(
                    'email' => $params['email'],
                    'firstName' => 'KBC',
                    'lastName' => $params['email'],
                    'password' => md5(uniqid() . str_repeat($params['email'], 2)),
                );

                $result = $childJob->run($job, $childParams, $restApi);
                if (!empty($result['uid'])) {
                    $userId = $result['uid'];
                }
            }
        }

        if ($userId) {
            $restApi->addUserToProject($userId, $params['pid'], RestApi::$userRoles[$params['role']]);

            $this->configuration->saveProjectUser($params['pid'], $params['email'], $params['role'], false);
        } else {
            $restApi->inviteUserToProject($params['email'], $params['pid'], RestApi::$userRoles[$params['role']]);

            $this->configuration->saveProjectUser($params['pid'], $params['email'], $params['role'], true);
        }

        $result = [];
        if ($userId) {
            $result['flags'] = array('invitation' => $this->translator->trans('result.flag.invitation'));
        }
    }
}
