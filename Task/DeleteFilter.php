<?php
/**
 * DeleteFilter.php
 *
 * @author: Miroslav Čillík <miro@keboola.com>
 * @created: 30.4.13
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\Exception\RestApiException;
use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\Syrup\Exception\UserException;

class DeleteFilter extends AbstractTask
{

    public function prepare($params)
    {
        $this->checkParams($params, ['writerId']);
        $this->checkWriterExistence($params['writerId']);

        if (isset($params['name'])) {
            if (!$this->configuration->getFilter($params['name'])) {
                throw new UserException($this->translator->trans('parameters.filters.not_exist %1', ['%1' => $params['name']]));
            }
        } else {
            //@TODO backwards compatibility, REMOVE SOON
            $this->checkParams($params, ['uri']);
            if (!$this->configuration->checkFilterUri($params['uri'])) {
                throw new UserException($this->translator->trans('parameters.filters.not_exist %1', ['%1' => $params['uri']]));
            }
        }

        $result = [];
        if (isset($params['name'])) {
            $result['name'] = $params['name'];
        } else {
            $result['uri'] = $params['uri'];
        }
        return $result;
    }

    /**
     * required: uri|name
     * optional:
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        $uris = [];
        if (isset($params['name'])) {
            // Delete filter in all projects
            foreach ($this->configuration->getFiltersProjectsByFilter($params['name']) as $fp) {
                $uris[] = $fp['uri'];
            }
        } else {
            // Delete filter only from particular project
            $this->checkParams($params, ['uri']);
            if (!$this->configuration->checkFilterUri($params['uri'])) {
                throw new UserException($this->translator->trans('parameters.filters.not_exist %1', ['%1' => $params['uri']]));
            }
            $uris[] = $params['uri'];
        }

        $bucketAttributes = $this->configuration->bucketAttributes();
        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

        foreach ($uris as $uri) {
            try {
                $this->restApi->deleteFilter($uri);
            } catch (RestApiException $e) {
                $message = json_decode($e->getMessage(), true);
                if (!isset($message['error']['errorClass']) || $message['error']['errorClass'] != 'GDC::Exception::NotFound') {
                    throw $e;
                }
            }
        }

        if (isset($params['name'])) {
            $this->configuration->deleteFilter($params['name']);
        } else {
            $this->configuration->deleteFilterFromProject($params['uri']);
        }

        return [];
    }
}
