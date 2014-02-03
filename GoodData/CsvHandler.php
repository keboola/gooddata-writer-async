<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-08-06
 */

namespace Keboola\GoodDataWriter\GoodData;

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


	public function __construct($scriptsPath, $s3Client, $tmpDir, $jobId)
	{
		$this->scriptPath = $scriptsPath . '/convert_csv.php';
		$this->s3Client = $s3Client;
		$this->tmpDir = $tmpDir;
		$this->jobId = $jobId;
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
			escapeshellarg('X-KBC-RunId: ' . $this->jobId), escapeshellarg($userAgent), escapeshellarg($sapiUrl));
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

		$process = new Process($this->command);
		$process->setTimeout(null);
		$process->run();
		if (!$process->isSuccessful()) {
			$message = 'CSV handling failed. ' . $process->getErrorOutput();
			$e = new CsvHandlerException($message);
			$e->setData(array('command' => $this->command));
			if (!$process->getErrorOutput()) {
				$e->setData(array(
					'priority' => 'alert',
					'command' => $this->command,
					'error' => 'No error output'
				));
			}
			throw $e;
		}

		$this->command = null;
	}

}