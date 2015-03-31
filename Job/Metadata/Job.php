<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */

namespace Keboola\GoodDataWriter\Job\Metadata;

use Keboola\GoodDataWriter\Aws\S3Client;

class Job extends \Keboola\Syrup\Job\Metadata\Job
{
    const PRIMARY_QUEUE = 'primary';
    const SECONDARY_QUEUE = 'secondary';
    const SERVICE_QUEUE = 'service';

    public function setWriterId($writerId)
    {
        $this->data['params']['writerId'] = $writerId;
    }

    public function addTask($name, array $params = [], $definition = null)
    {
        $this->data['params']['tasks'][] = [
            'name' => $name,
            'params' => $params,
            'definition' => $definition
        ];
    }

    public function uploadDefinitions(S3Client $s3Client)
    {
        foreach ($this->data['params']['tasks'] as $i => &$task) {
            if (isset($task['definition'])) {
                $task['definition'] = $s3Client->uploadString(
                    sprintf('%d/%d/definition.json', $this->getId(), $i),
                    json_encode($task['definition'])
                );
            }
        }
    }

    public static function isJobFinished($status)
    {
        return !in_array($status, [self::STATUS_WAITING, self::STATUS_PROCESSING]);
    }
}
