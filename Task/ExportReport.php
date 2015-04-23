<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Task;

use Keboola\Csv\CsvFile;
use Keboola\GoodDataWriter\Exception\RestApiException;
use Aws\Common\Client as AwsClient;
use Keboola\GoodDataWriter\Job\Metadata\Job;
use Keboola\GoodDataWriter\Writer\Configuration;
use Keboola\StorageApi\Table;
use Keboola\Syrup\Exception\UserException;

class ExportReport extends AbstractTask
{

    public function prepare($params)
    {
        $this->checkParams($params, ['writerId', 'pid', 'report', 'table']);
        $this->checkWriterExistence($params['writerId']);

        return [
            'pid' => $params['pid'],
            'report' => $params['report'],
            'table' => $params['table']
        ];
    }

    /**
     * @TODO works only with main project
     * required: report, table
     * optional:
     */
    public function run(Job $job, $taskId, array $params = [], $definitionFile = null)
    {
        $this->initRestApi($job);
        $this->checkParams($params, ['report', 'table']);

        $bucketAttributes = $this->configuration->getBucketAttributes();
        $this->configuration->checkTable(Configuration::PROJECTS_TABLE_NAME);

        if (!preg_match('/^([^\.]+)\.([^\.]+)\.([^\.]+)$/', $params['table'])) {
            throw new UserException($this->translator->trans('parameters.report.table_not_valid %1', ['%1' => $params['table']]));
        }

        $this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

        $report = $this->restApi->get($params['report']);

        if (!isset($report['report']['content']['definitions'][0])) {
            throw new RestApiException($this->translator->trans('parameters.report.no_definitions %1', ['%1' => $params['report']]));
        }
        $reportDefinitions = $report['report']['content']['definitions'];
        $reportDefinitionUri = array_pop($reportDefinitions);

        $response = $this->restApi->executeReportRaw($bucketAttributes['gd']['pid'], $reportDefinitionUri);
        $csvUri = $response['uri'];

        /** @TODO Streamed import to SAPI
        $stream = $this->restApi->getStream($csvUri);
        $this->uploadToS3($stream->getStream());
         */

        $filename = $this->getTmpDir($job->getId()) . '/' . uniqid("report-export", true) .'.csv';

        if (false !== $this->restApi->getToFile($csvUri, $filename)) {
            $this->uploadToSapi($filename, $params['table']);
        } else {
            $this->logger->warn("Report export timed out");
        }

        return [];
    }

    protected function uploadToSapi($filename, $tableId)
    {
        $normalizedCsv = $this->normalizeCsv($filename);

        $csvFile = new CsvFile($normalizedCsv);

        list($stage, $bucket, $tableName) = explode('.', $tableId);
        try {
            $this->storageApiClient->createTableAsync($stage . '.'. $bucket, $tableName, $csvFile);
        } catch (\Exception $e) {
            $this->storageApiClient->writeTableAsync($tableId, $csvFile);
        }
    }

    protected function normalizeCsv($filePath)
    {
        $fr = fopen($filePath, 'r');

        $destPath = str_replace('.csv', '-normalized.csv', $filePath);

        $fw = fopen($destPath, 'w');

        $cnt = 0;
        $header = null;

        while ($line = fgets($fr)) {
            $lineArr = Table::csvStringToArray($line);
            $lineArr = $lineArr[0];

            if ($cnt == 0) {
                $lineArr = Table::normalizeHeader($lineArr);
            }

            foreach ($lineArr as $k => $v) {
                $v = str_replace('"', '', $v);
                $lineArr[$k] = '"' . $v . '"';
            }

            $line = implode(',', $lineArr) . PHP_EOL;

            fwrite($fw, $line);
            $cnt++;
        }

        fclose($fw);
        fclose($fr);

        return $destPath;
    }
}