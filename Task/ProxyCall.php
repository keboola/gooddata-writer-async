<?php
/**
 * Proxy Call Job
 *
 * User: mirocillik
 * Date: 19/11/13
 * Time: 15:00
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\Job\Metadata\Job;

class ProxyCall extends AbstractTask
{

    public function prepare($params)
    {
        $this->checkParams($params, ['writerId', 'query', 'payload']);
        $this->checkWriterExistence($params['writerId']);
        return [
            'query' => $params['query'],
            'payload' => $params['payload']
        ];
    }

    /**
     * required: query, payload
     * optional: pid
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        $this->checkParams($params, ['query', 'payload']);

        $bucketAttributes = $this->configuration->bucketAttributes();
        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
        $response = $this->restApi->post($params['query'], $params['payload']);

        return [
            'response' => $response
        ];
    }
}
