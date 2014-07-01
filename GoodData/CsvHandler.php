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

class CsvHandlerException extends ClientException
{

}

class CsvHandlerNetworkException extends ClientException
{

}

class CsvHandler
{
	private $scriptPath;
	private $storageApiClient;
	private $logger;

	private $jobId;
	private $runId;


	public function __construct($scriptsPath, Client $storageApi, Logger $logger)
	{
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

		$result = $this->storageApiClient->exportTableAsync($tableId, $params);
		if (empty($result['file']['id'])) {
			throw new \Exception('Async export from SAPI returned bad response: '. json_encode($result));
		}

		return $result['file']['id'];
	}

	/**
	 * Assemble curl to download csv from SAPI and ungzip it
	 * There is approx. 38 minutes backoff for 5xx errors from SAPI (via --retry 12)
	 */
	public function initDownload($fileId)
	{
		// get file's s3 url
		$result = $this->storageApiClient->getFile($fileId);
		if (empty($result['url'])) {
			throw new \Exception('Get file after async export from SAPI returned bad response: '. json_encode($result));
		}

		return sprintf('curl -s -S -f --header %s --retry 12 %s | gzip -d', escapeshellarg('Accept-encoding: gzip'), escapeshellarg($result['url']));
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

	public function getHeaders($definition)
	{
		$csvHeaders = array();
		foreach ($definition['columns'] as $column) {
			$csvHeaders[] = $column['name'];
			if ($column['type'] == 'DATE') {
				$csvHeaders[] = $column['name'] . '_dt';
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
	public function prepareTransformation($definition)
	{
		$dateColumnsIndices = array();
		$timeColumnsIndices = array();
		$i = 1;
		foreach ($definition['columns'] as $column) {
			if ($column['type'] == 'DATE') {
				$dateColumnsIndices[] = $i;
				if ($column['includeTime']) {
					$timeColumnsIndices[] = $i;
				}
			}
			$i++;
		}

		if (!count($dateColumnsIndices)) {
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
	public function runUpload($username, $password, $fileUrl, $definition, $tableId, $incrementalLoad = false, $filterColumn = false, $filterValue = null)
	{
		$definition = $this->removeIgnoredColumnsFromDefinition($definition);
		$headersCommand = '"' . implode('","', $this->getHeaders($definition)) . '"';
		$transformationCommand = $this->prepareTransformation($definition);

		$uploadCommand = sprintf('gzip -c | curl -s -S -T - --header %s --retry 12 --user %s:%s %s',
			escapeshellarg('Content-encoding: gzip'), escapeshellarg($username), escapeshellarg($password),
			escapeshellarg($fileUrl));
		/*$uploadCommand = sprintf('curl -s -S -T - --retry 12 --user %s:%s %s',
			escapeshellarg($username), escapeshellarg($password), escapeshellarg($fileUrl));*/

		$columns = array();
		foreach ($definition['columns'] as $c) {
			$columns[] = $c['name'];
		}
		$fileId = $this->exportTable($tableId, $columns, $incrementalLoad, $filterColumn, $filterValue);

		$appError = false;
		$errors = array();
		$currentError = null;
		for ($i = 0; $i < 10; $i++) {
			if ($transformationCommand)
				$command = '(echo ' . escapeshellarg($headersCommand) . '; ' . $this->initDownload($fileId) . ' | tail -n +2 | ' .  $transformationCommand . ') | ' . $uploadCommand;
			else
				$command = '(echo ' . escapeshellarg($headersCommand) . '; ' . $this->initDownload($fileId) . ' | tail -n +2) | ' . $uploadCommand;

			$process = new Process($command);
			$process->setTimeout(null);
			$process->run();
			$currentError = $process->getErrorOutput();

			if ($currentError) {
				$curlError = strpos($currentError, 'curl: (') !== false;
				if ($curlError) {
					$appError = true;
				} else {
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
				'error' => str_replace("\ngzip: stdin: unexpected end of file\n", "", $currentError),
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
			$e = end($errors);
			$error = ($e && isset($e['error']))? $e['error'] : $currentError;
			throw new CsvHandlerException('CSV handling failed. ' . $error);
		}
	}

}