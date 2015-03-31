<?php
/**
 * @author Erik Zigo <erik@keboola.com>
 * @date 2013-07-12
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\Syrup\Exception\UserException;

class RemoveUserFromProject extends AbstractTask
{

    public function prepare($params)
    {
        $this->checkParams($params, ['writerId', 'pid', 'email']);
        $this->checkWriterExistence($params['writerId']);
        if (!$this->configuration->getProject($params['pid'])) {
            throw new UserException($this->translator->trans('parameters.pid_not_configured'));
        }
        if (!$this->configuration->isProjectUser($params['email'], $params['pid'])) {
            throw new UserException($this->translator->trans('parameters.email_not_configured'));
        }
        $this->configuration->checkTable(Configuration::PROJECTS_TABLE_NAME);
        $this->configuration->checkTable(Configuration::PROJECT_USERS_TABLE_NAME);

        return [
            'pid' => $params['pid'],
            'email' => $params['email']
        ];
    }

    /**
     * required: pid, email
     * optional:
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        $this->checkParams($params, ['pid', 'email']);
        $params['email'] = strtolower($params['email']);

        $this->restApi->login($this->getDomainUser()->username, $this->getDomainUser()->password);

        if (!$this->configuration->isProjectUser($params['email'], $params['pid'])) {
            throw new UserException($this->translator->trans('parameters.email_not_configured_in_project'));
        }

        $userId = false;
        $user = $this->configuration->getUser($params['email']);
        if ($user && $user['uid']) {
            $userId = $user['uid'];
        }

        // find user in domain
        if (!$userId) {
            $userId = $this->restApi->userId($params['email'], $this->getDomainUser()->domain);

            if ($userId) {
                $this->configuration->saveUser($params['email'], $userId);
            }
            //@TODO save user invite to configuration if user came from invitation
        }

        // find user in project (maybe invited)
        if (!$userId) {
            $userId = $this->restApi->userIdByProject($params['email'], $params['pid']);
        }

        if ($userId) {
            $this->restApi->removeUserFromProject($userId, $params['pid']);
        }

        // cancel possible invitations
        $this->restApi->cancelInviteUserToProject($params['email'], $params['pid']);

        $this->configuration->deleteProjectUser($params['pid'], $params['email']);

        return [];
    }
}
