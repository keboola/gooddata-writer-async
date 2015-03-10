<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\Exception\JobProcessException;
use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\Exception\UserAlreadyExistsException;
use Keboola\GoodDataWriter\Job\Metadata\Job;

class CreateUser extends AbstractTask
{

    public function prepare($params)
    {
        $this->checkParams($params, ['writerId', 'firstName', 'lastName', 'email', 'password']);
        $this->checkWriterExistence($params['writerId']);
        if (strlen($params['password']) < 7) {
            throw new WrongParametersException($this->translator->trans('parameters.password_length'));
        }
        $this->configuration->checkUsersTable();

        return [
            'firstName' => $params['firstName'],
            'lastName' => $params['lastName'],
            'email' => $params['email'],
            'password' => $params['password'],
            'ssoProvider' => empty($params['ssoProvider'])? null : $params['ssoProvider']
        ];
    }

    /**
     * required: email, password, firstName, lastName
     * optional: ssoProvider
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        $this->checkParams($params, ['email', 'password', 'firstName', 'lastName']);
        $params['email'] = strtolower($params['email']);

        $this->restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);
        $alreadyExists = false;
        try {
            $userId = $this->restApi->createUser(
                $this->getDomainUser()->domain,
                $params['email'],
                $params['password'],
                $params['firstName'],
                $params['lastName'],
                $this->gdSsoProvider
            );
        } catch (UserAlreadyExistsException $e) {
            $userId = $e->getMessage();
            $alreadyExists = true;
            if (!$userId) {
                throw new JobProcessException($this->translator->trans('error.user.in_other_domain'));
            }
        }

        $this->configuration->saveUser($params['email'], $userId);
        if (!$alreadyExists) {
            $this->sharedStorage->saveUser($this->configuration->projectId, $this->configuration->writerId, $userId, $params['email']);
        }

        return [
            'uid' => $userId,
            'alreadyExists' => $alreadyExists
        ];
    }
}
