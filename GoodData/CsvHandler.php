<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-08-06
 */

namespace Keboola\GoodDataWriter\GoodData;

use Monolog\Logger;
use Symfony\Component\Process\Process;
use Keboola\GoodDataWriter\Exception\ClientException;

class CsvHandlerException extends ClientException
{

}

class CsvHandler
{

	private $scriptPath;
	private $tmpDir;
	private $command;

	/**
	 * @var \Keboola\GoodDataWriter\Service\S3Client
	 */
	private $s3Client;
	private $logger;

	private $jobId;
	private $runId;


	public function __construct($scriptsPath, $s3Client, $tmpDir, Logger $logger)
	{
		$this->scriptPath = $scriptsPath . '/convert_csv.php';
		$this->s3Client = $s3Client;
		$this->tmpDir = $tmpDir;
		$this->logger = $logger;
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
	public function initDownload($tableId, $token, $sapiUrl, $userAgent, $incrementalLoad = false, $filterColumn = false, $filterValue = null)
	{
		$incrementalLoad = $incrementalLoad ? '&changedSince=-' . $incrementalLoad . '+days' : null;
		$filter = ($filterColumn && $filterValue) ? '&whereColumn=' . $filterColumn . '&whereValues%5B%5D=' . $filterValue : null;
		$sapiUrl = sprintf('%s/v2/storage/tables/%s/export?format=escaped%s%s', $sapiUrl, $tableId, $incrementalLoad, $filter);
		$this->command = sprintf('curl -s -S -f --header %s --header %s --header %s --user-agent %s --retry 12 %s | gzip -d',
			escapeshellarg('Accept-encoding: gzip'), escapeshellarg('X-StorageApi-Token: ' . $token),
			escapeshellarg('X-KBC-RunId: ' . $this->runId), escapeshellarg($userAgent), escapeshellarg($sapiUrl));
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

		$command = sprintf('curl -s -S -T - --retry 12 --user %s:%s %s',
			escapeshellarg($username), escapeshellarg($password),
			escapeshellarg($url . $uri . '/data.csv'));
		/*$command = sprintf('gzip -c | curl -s -S -T - --header %s --retry 12 --user %s:%s %s',
			escapeshellarg('Content-encoding: gzip'), escapeshellarg($username), escapeshellarg($password),
			escapeshellarg($url . $uri . '/data.csv'));*/

		$this->command .= ' | ' . $command;

		$error = null;
		for ($i = 0; $i < 5; $i++) {
			$process = new Process($this->command);
			$process->setTimeout(null);
			$process->run();
			$error = $process->getErrorOutput();

			if ($process->isSuccessful() && !$error) {
				$this->command = null;
				return;
			} else {
				$this->logger->error('Curl error during csv handling', array(
					'command' => $this->command,
					'error' => $error ? $error : 'No error output',
					'jobId' => $this->jobId,
					'runId' => $this->runId,
				));

				$retry = false;
				if (substr($error, 0, 7) == 'curl: (') {
					$curlErrorNumber = substr($error, 7, strpos($error, ')') - 7);
					if (in_array((int)$curlErrorNumber, array(18, 52, 55))) {
						// Retry for curl 18 (CURLE_PARTIAL_FILE), 52 (CURLE_GOT_NOTHING) and 55 (CURLE_SEND_ERROR)
						$retry = true;
					}
				}
				if (!$retry) {
					break;
				}
			}

			sleep($i * 60);
		}

		$e = new CsvHandlerException('CSV handling failed. ' . $error);
		if ($error && substr($error, 0, 7) == 'curl: (') {
			$this->logger->alert('Curl error during csv handling', array(
				'command' => $this->command,
				'error' => $error,
				'jobId' => $this->jobId,
				'runId' => $this->runId,
			));
		}
		if (!$error) {
			$this->logger->alert('Curl error during csv handling', array(
				'command' => $this->command,
				'error' => 'No error output',
				'jobId' => $this->jobId,
				'runId' => $this->runId,
			));
		}
		$this->command = null;
		throw $e;
	}

}