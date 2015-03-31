<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-05-07
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\GoodDataWriter\Job\Metadata\Job;

class ResetTable extends AbstractTask
{

    public function prepare($params)
    {
        $this->checkParams($params, ['writerId', 'tableId']);
        $this->checkWriterExistence($params['writerId']);

        return [
            'tableId' => $params['tableId']
        ];
    }

    /**
     * required: tableId
     * optional:
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        $this->checkParams($params, ['tableId']);

        $bucketAttributes = $this->configuration->getBucketAttributes();

        $tableDefinition = $this->configuration->getDataSet($params['tableId']);

        $projects = $this->configuration->getProjects();

        $result = [];

        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

        $updateOperations = [];
        foreach ($projects as $project) {
            if ($project['active']) {
                $result = $this->restApi->dropDataSet($project['pid'], $tableDefinition['title']);
                if ($result) {
                    $updateOperations[$project['pid']] = $result;
                }
            }
        }
        if (count($updateOperations)) {
            $result['info'] = $updateOperations;
        }

        $this->configuration->updateDataSetDefinition($params['tableId'], 'isExported', 0);

        return $result;
    }
}
