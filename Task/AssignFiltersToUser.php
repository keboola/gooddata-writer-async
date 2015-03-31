<?php
/**
 * @author Miroslav Cillik <miro@keboola.com>
 * @date 2013-04-24
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\Syrup\Exception\UserException;

class AssignFiltersToUser extends AbstractTask
{

    public function prepare($params)
    {
        //@TODO backwards compatibility, REMOVE SOON
        if (isset($params['userEmail'])) {
            $params['email'] = $params['userEmail'];
            unset($params['userEmail']);
        }
        ////

        if (is_array($params['email'])) {
            throw new UserException($this->translator->trans('parameters.filters.email_is_array'));
        }

        $this->checkParams($params, ['writerId', 'email']);
        if (!isset($params['filters'])) {
            throw new UserException($this->translator->trans('parameters.filters.required'));
        }
        $configuredFilters = [];
        foreach ($this->configuration->getFilters() as $f) {
            $configuredFilters[] = $f['name'];
        }
        if (is_array($params['filters'])) {
            foreach ($params['filters'] as $f) {
                if (!in_array($f, $configuredFilters)) {
                    $filters = is_array($f)? implode(', ', $f) : $f;
                    throw new UserException($this->translator->trans('parameters.filters.not_exist %1', ['%1' => $filters]));
                }
            }
        } else {
            throw new UserException($this->translator->trans('parameters.filters.not_array'));
        }
        $this->checkWriterExistence($params['writerId']);

        return [
            'filters' => $params['filters'],
            'email' => $params['email']
        ];
    }

    /**
     * required: email, filters
     * optional:
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);

        $this->checkParams($params, ['email']);
        $params['email'] = strtolower($params['email']);

        if (!is_array($params['filters'])) {
            throw new UserException($this->translator->trans('configuration.filters.not_array'));
        }

        $user = $this->configuration->getUser($params['email']);
        if ($user == false) {
            throw new UserException($this->translator->trans('parameters.email_not_configured'));
        }

        $configuredFilters = [];
        foreach ($this->configuration->getFilters() as $f) {
            $configuredFilters[] = $f['name'];
        }

        $pidUris = [];
        foreach ($params['filters'] as $name) {
            if (!in_array($name, $configuredFilters)) {
                throw new UserException($this->translator->trans('parameters.filters.not_exist %1', ['%1' => $name]));
            }
            foreach ($this->configuration->getFiltersProjectsByFilter($name) as $fp) {
                if (!$fp['uri']) {
                    throw new UserException($this->translator->trans('configuration.filter.missing_uri %1', ['%1' => $name]));
                }
                $pidUris[$fp['pid']][] = $fp['uri'];
            }
        }

        $bucketAttributes = $this->configuration->getBucketAttributes();
        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

        foreach ($pidUris as $pid => $uris) {
            $this->restApi->assignFiltersToUser($uris, $user['uid'], $pid);
        }
        $this->configuration->saveFiltersUsers($params['filters'], $params['email']);

        return [];
    }
}
