<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-08-06
 */

namespace Keboola\GoodDataWriter\GoodData;

use Keboola\StorageApi\Client;
use Monolog\Logger;
use Symfony\Component\Process\Process;
use Keboola\GoodDataWriter\Exception\ClientException;
use Syrup\ComponentBundle\Filesystem\Temp;
use Keboola\StorageApi\TableExporter;

class CsvHandlerException extends ClientException
{

}

class CsvHandlerNetworkException extends ClientException
{

}

class CsvHandler
{
	/**
	 * @var Temp
	 */
	private $temp;
	private $scriptPath;
	/**
	 * @var Client $storageApi
	 */
	private $storageApiClient;
	/**
	 * @var Logger $logger
	 */
	private $logger;

	private $jobId;
	private $runId;


	public function __construct(Temp $temp, $scriptsPath, Client $storageApi, Logger $logger)
	{
		$this->temp = $temp;
		$this->scriptPath = $scriptsPath . '/convert_csv.php';
		$this->storageApiClient = $storageApi;
		$this->logger = $logger;

		if (!file_exists($this->scriptPath))
			throw new \Exception('Conversion script for csv handling in pipe does not exist: ' . $this->scriptPath);
	}

	public function setJobId($id)
	{
		$this->jobId = $id;
	}

	public function setRunId($id)
	{
		$this->runId = $id;
	}

	public function exportTable($tableId, $columns, $incrementalLoad = false, $filterColumn = false, $filterValue = null)
	{
		$params = array(
			'gzip' => true,
			'format' => 'escaped',
			'columns' => $columns
		);
		if ($incrementalLoad) {
			$params['changedSince'] = '-' . $incrementalLoad . ' days';
		}
		if ($filterColumn && $filterValue) {
			$params['whereColumn'] = $filterColumn;
			$params['whereValues'] = array($filterValue);
		}

		$file = $this->temp->createTmpFile();
		$fileName = $file->getRealPath();
		$exporter = new TableExporter($this->storageApiClient);
		$exporter->exportTable($tableId, $fileName, $params);

		return $fileName;
	}

	/**
	 * Get column names without ignores
	 */
	public function removeIgnoredColumnsFromDefinition($definition)
	{
		$result = array();
		foreach ($definition['columns'] as $column) {
			if ($column['type'] != 'IGNORE') {
				$result[] = $column;
			}
		}

		$definition['columns'] = $result;
		return $definition;
	}

	public function getHeaders($definition, $noDateFacts=false)
	{
		$csvHeaders = array();
		foreach ($definition['columns'] as $column) {
			$csvHeaders[] = $column['name'];
			if ($column['type'] == 'DATE') {
				if (!$noDateFacts) {
					$csvHeaders[] = $column['name'] . '_dt';
				}
				if ($column['includeTime']) {
					$csvHeaders[] = $column['name'] . '_tm';
					$csvHeaders[] = $column['name'] . '_id';
				}
			}
		}

		return $csvHeaders;
	}


	/**
	 * Parse csv and prepare for data load
	 */
	public function prepareTransformation($definition, $noDateFacts=false)
	{
		$dateColumnsIndices = array();
		$timeColumnsIndices = array();
		$i = 1;
		foreach ($definition['columns'] as $column) {
			if ($column['type'] == 'DATE') {
				if (!$noDateFacts)
					$dateColumnsIndices[] = $i;
				if ($column['includeTime']) {
					$timeColumnsIndices[] = $i;
				}
			}
			$i++;
		}

		if (!count($dateColumnsIndices) && !count($timeColumnsIndices)) {
			return false;
		}

		// Add column headers according to manifest, calculate date facts and remove ignored columns
		$command  = 'php ' . escapeshellarg($this->scriptPath);
		if (count($dateColumnsIndices)) {
			$command .= ' -d' . implode(',', $dateColumnsIndices);
		}
		if (count($timeColumnsIndices)) {
			$command .= ' -t' . implode(',', $timeColumnsIndices);
		}

		return $command;
	}


	/**
	 * Assemble curl to upload wo GD WebDav and run whole command
	 * There is approx. 38 minutes backoff for 5xx errors from WebDav (via --retry 12)
	 */
	public function runUpload($username, $password, $fileUrl, $definition, $tableId, $incrementalLoad=false, $filterColumn=false, $filterValue=null, $noDateFacts=false)
	{
		$definition = $this->removeIgnoredColumnsFromDefinition($definition);
		$headersCommand = '"' . implode('","', $this->getHeaders($definition, $noDateFacts)) . '"';

		$columns = array();
		foreach ($definition['columns'] as $c) {
			$columns[] = $c['name'];
		}
		$filePath = $this->exportTable($tableId, $columns, $incrementalLoad, $filterColumn, $filterValue);

		$transformationCommand = $this->prepareTransformation($definition, $noDateFacts);
		$uploadCommand = sprintf('gzip -c | curl -s -S -T - --header %s --retry 12 --user %s:%s %s',
			escapeshellarg('Content-encoding: gzip'), escapeshellarg($username), escapeshellarg($password),
			escapeshellarg($fileUrl));
		/*$uploadCommand = sprintf('curl -s -S -T - --retry 12 --user %s:%s %s',
			escapeshellarg($username), escapeshellarg($password), escapeshellarg($fileUrl));*/

		$command = '(echo ' . escapeshellarg($headersCommand) . '; cat ' . escapeshellarg($filePath) . ' | gzip -d | tail -n +2 ';
		if ($transformationCommand)
			$command .= '| ' .  $transformationCommand;
		$command .= ') | ' . $uploadCommand;


		$appError = false;
		$errors = array();
		$currentError = null;
		for ($i = 0; $i < 10; $i++) {
			$process = new Process($command);
			$process->setTimeout(null);
			$process->run();
			$currentError = $process->getErrorOutput();

			if ($currentError) {
				$curlError = strpos($currentError, 'curl: (') !== false;
				if ($curlError) {
					$appError = true;
				} else {
					$errors[] = array(
						'error' => str_replace(array(
							"\ngzip: stdin: unexpected end of file\n",
							"tail: write error: Broken pipe"
						), "", $currentError),
						'command' => str_replace($password, '***', $command)
					);
					break;
				}
			} else {
				if ($process->isSuccessful()) {
					return;
				} else {
					$appError = true;
				}
			}

			$error = array(
				'error' => str_replace(array(
					"\ngzip: stdin: unexpected end of file\n",
					"tail: write error: Broken pipe"
				), "", $currentError),
				'command' => str_replace($password, '***', $command),
				'retry' => $i+1,
				'jobId' => $this->jobId,
				'runId' => $this->runId
			);
			$this->logger->error('csv transfer backoff', $error);
			$error['date'] = date('c');
			$errors[] = $error;
			sleep(30);
		}

		if ($appError) {
			$e = new CsvHandlerNetworkException('Network Error');
			$e->setData(array(
				'log' => $errors,
				'jobId' => $this->jobId,
				'runId' => $this->runId,
			));
			throw $e;
		} else {
			$lastError = end($errors);
			$error = ($lastError && isset($lastError['error']))? $lastError['error'] : $currentError;
			$e = new CsvHandlerException('CSV handling failed. ' . $error);
			if (isset($error['command'])) {
				$e->setData(array(
					'command' => $lastError['command']
				));
			}
			throw $e;
		}
	}

}