<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */
namespace Keboola\GoodDataWriter\Elasticsearch;

use Elasticsearch\Client;

class Search extends \Keboola\Syrup\Elasticsearch\Search
{

    private $componentName;

    public function __construct(Client $client, $indexPrefix, $componentName)
    {
        $this->client = $client;
        $this->indexPrefix = $indexPrefix;
        $this->componentName = $componentName;
    }

    public function getJobs(array $params)
    {
        $params['component'] = $this->componentName;
        return parent::getJobs($params);
    }
}
