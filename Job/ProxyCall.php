<?php
/**
 * Proxy Call Job
 *
 * User: mirocillik
 * Date: 19/11/13
 * Time: 15:00
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\GoodData\RestApi;

class ProxyCall extends AbstractJob
{

    public function prepare($params)
    {
        $this->checkParams($params, array('writerId', 'query', 'payload'));
        $this->checkWriterExistence($params['writerId']);
        return array(
            'query' => $params['query'],
            'payload' => $params['payload']
        );
    }

    /**
     * required: query, payload
     * optional: pid
     */
    public function run($job, $params, RestApi $restApi)
    {
        $this->checkParams($params, array('query', 'payload'));

        $bucketAttributes = $this->configuration->bucketAttributes();
        $restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
        $response = $restApi->post($params['query'], $params['payload']);

        return array(
            'response' => $response
        );
    }
}
