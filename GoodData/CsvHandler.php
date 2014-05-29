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

class CsvHandler
{
	private $scriptPath;
	private $storageApiClient;

	private $jobId;
	private $runId;

	private $command;


	public function __construct($scriptsPath, Client $storageApi)
	{
		$this->scriptPath = $scriptsPath . '/convert_csv.php';
		$this->storageApiClient = $storageApi;

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

	/**
	 * Assemble curl to download csv from SAPI and ungzip it
	 * There is approx. 38 minutes backoff for 5xx errors from SAPI (via --retry 12)
	 */
	public function initDownload($tableId, $incrementalLoad = false, $filterColumn = false, $filterValue = null)
	{
		// run async export and get file id
		$params = array(
			'gzip' => true,
			'format' => 'escaped'
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

		// get file's s3 url
		$result = $this->storageApiClient->getFile($result['file']['id']);
		if (empty($result['url'])) {
			throw new \Exception('Get file after async export from SAPI returned bad response: '. json_encode($result));
		}

		$this->command = sprintf('curl -s -S -f --header %s --retry 12 %s | gzip -d', escapeshellarg('Accept-encoding: gzip'), escapeshellarg($result['url']));
	}


	/**
	 * Parse csv and prepare for data load
	 */
	public function prepareTransformation($definition)
	{
		if (!$this->command) {
			throw new CsvHandlerException('You must init the download first');
		}

		$csvHeaders = array();
		$ignoredColumnsIndices = array();
		$dateColumnsIndices = array();
		$timeColumnsIndices = array();
		$i = 1;
		foreach ($definition['columns'] as $column) {
			$gdName = null;
			switch ($column['type']) {
				case 'CONNECTION_POINT':
					$csvHeaders[] = $column['name'];
					break;
				case 'FACT':
					$csvHeaders[] = $column['name'];
					break;
				case 'ATTRIBUTE':
					$csvHeaders[] = $column['name'];
					break;
				case 'LABEL':
				case 'HYPERLINK':
					$csvHeaders[] = $column['name'];
					break;
				case 'REFERENCE':
					$csvHeaders[] = $column['name'];
					break;
				case 'DATE':
					$csvHeaders[] = $column['name'];
					$csvHeaders[] = $column['name'] . '_dt';
					if ($column['includeTime']) {
						$csvHeaders[] = $column['name'] . '_tm';
						$csvHeaders[] = $column['name'] . '_id';

						$timeColumnsIndices[] = $i;
					}
					$dateColumnsIndices[] = $i;
					break;
				case 'IGNORE':
					$ignoredColumnsIndices[] = $i;
					break;
			}

			$i++;
		}


		// Add column headers according to manifest, calculate date facts and remove ignored columns
		$command  = 'php ' . escapeshellarg($this->scriptPath);
		$command .= ' -h' . implode(',', $csvHeaders);
		if (count($dateColumnsIndices)) {
			$command .= ' -d' . implode(',', $dateColumnsIndices);
		}
		if (count($timeColumnsIndices)) {
			$command .= ' -t' . implode(',', $timeColumnsIndices);
		}
		if (count($ignoredColumnsIndices)) {
			$command .= ' -i' . implode(',', $ignoredColumnsIndices);
		}

		$this->command .= ' | ' . $command;
	}


	/**
	 * Assemble curl to upload wo GD WebDav and run whole command
	 * There is approx. 38 minutes backoff for 5xx errors from WebDav (via --retry 12)
	 */
	public function runUpload($username, $password, $url, $uri)
	{
		if (!$this->command) {
			throw new CsvHandlerException('You must init the download first');
		}

		if (substr($url, 0, 8) != 'https://') {
			$url = 'https://' . $url;
		}
		$urlParts = parse_url($url);
		$url = 'https://' . $urlParts['host'];

		/*$command = sprintf('gzip -c | curl -s -S -T - --header %s --retry 12 --user %s:%s %s',
			escapeshellarg('Content-encoding: gzip'), escapeshellarg($username), escapeshellarg($password),
			escapeshellarg($url . $uri . '/data.csv'));*/
		$command = sprintf('curl -s -S -T - --retry 12 --user %s:%s %s',
			escapeshellarg($username), escapeshellarg($password),
			escapeshellarg($url . $uri . '/data.csv'));

		$this->command .= ' | ' . $command;

		$error = null;
		for ($i = 0; $i < 10; $i++) {
			$process = new Process($this->command);
			$process->setTimeout(null);
			$process->run();
			$error = $process->getErrorOutput();

			if ($process->isSuccessful() && !$error) {
				$this->command = null;
				return;
			} else {
				$retry = false;
				if (substr($error, 0, 7) == 'curl: (') {
					// Retry after any curl error
					$retry = true;
				}
				if (!$retry) {
					break;
				}
			}

			sleep($i * 60);
		}

		if (!$error || substr($error, 0, 7) == 'curl: (') {
			throw new \Exception(json_encode(array(
				'error' => 'Curl error during csv handling: ' . ($error? $error : 'No error output'),
				'command' => $this->command,
				'jobId' => $this->jobId,
				'runId' => $this->runId,
			)));
		} else {
			throw new CsvHandlerException('CSV handling failed. ' . $error);
		}
	}

}