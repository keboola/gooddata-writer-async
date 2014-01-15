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

	private $_timeDimensionManifestPath;
	private $_scriptPath;
	private $_tmpDir;
	private $_command;

	/**
	 * @var \Keboola\GoodDataWriter\Service\S3Client
	 */
	private $_s3Client;


	/**
	 * @param $scriptsPath
	 * @param $s3Client
	 * @param $tmpDir
	 * @param $jobId
	 */
	public function __construct($scriptsPath, $s3Client, $tmpDir, $jobId)
	{
		$this->_scriptPath = $scriptsPath . '/convert_csv.php';
		$this->_timeDimensionManifestPath = $scriptsPath . '/time-dimension-manifest.json';
		$this->_s3Client = $s3Client;
		$this->_tmpDir = $tmpDir;
		$this->_jobId = $jobId;
	}

	/**
	 * @param $tableId
	 * @param $token
	 * @param $sapiUrl
	 * @param $userAgent
	 * @param bool $incrementalLoad
	 * @param string|bool $filterColumn
	 * @param null $filterValue
	 *
	 * Assemble curl to download csv from SAPI and ungzip it
	 * There is approx. 38 minutes backoff for 5xx errors from SAPI (via --retry 12)
	 */
	public function initDownload($tableId, $token, $sapiUrl, $userAgent, $incrementalLoad = false, $filterColumn = false, $filterValue = null)
	{
		$incrementalLoad = $incrementalLoad ? '&changedSince=-' . $incrementalLoad . '+days' : null;
		$filter = ($filterColumn && $filterValue) ? '&whereColumn=' . $filterColumn . '&whereValues%5B%5D=' . $filterValue : null;
		$sapiUrl = sprintf('%s/v2/storage/tables/%s/export?format=escaped%s%s', $sapiUrl, $tableId, $incrementalLoad, $filter);
		$this->_command = sprintf('curl -s -S -f --header %s --header %s --header %s --user-agent %s --retry 12 %s | gzip -d',
			escapeshellarg('Accept-encoding: gzip'), escapeshellarg('X-StorageApi-Token: ' . $token),
			escapeshellarg('X-KBC-RunId: ' . $this->_jobId), escapeshellarg($userAgent), escapeshellarg($sapiUrl));
	}


	/**
	 * Parse csv and prepare for data load
	 * @param $xmlFileObject
	 * @throws CsvHandlerException
	 */
	public function prepareTransformation($xmlFileObject)
	{
		if (!$this->_command) {
			throw new CsvHandlerException('You must init the download first');
		}

		$csvHeaders = array();
		$ignoredColumnsIndices = array();
		$dateColumnsIndices = array();
		$timeColumnsIndices = array();
		$i = 1;
		foreach ($xmlFileObject->columns->column as $column) {
			$gdName = null;
			switch ((string)$column->ldmType) {
				case 'CONNECTION_POINT':
					$csvHeaders[] = (string)$column->name;
					break;
				case 'FACT':
					$csvHeaders[] = (string)$column->name;
					break;
				case 'ATTRIBUTE':
					$csvHeaders[] = (string)$column->name;
					break;
				case 'LABEL':
				case 'HYPERLINK':
					$csvHeaders[] = (string)$column->name;
					break;
				case 'REFERENCE':
					$csvHeaders[] = (string)$column->name;
					break;
				case 'DATE':
					$csvHeaders[] = (string)$column->name;
					$csvHeaders[] = (string)$column->name . '_dt';
					if ((string)$column->datetime == 'true') {
						$csvHeaders[] = (string)$column->name . '_tm';
						$csvHeaders[] = (string)$column->name . '_id';

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
		$command  = 'php ' . escapeshellarg($this->_scriptPath);
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

		$this->_command .= ' | ' . $command;
	}



	/**
	 * @param $username
	 * @param $password
	 * @param $url
	 * @throws CsvHandlerException
	 */
	public function runUpload($username, $password, $url)
	{
		if (!$this->_command) {
			throw new CsvHandlerException('You must init the download first');
		}

		$command = sprintf('gzip -c | curl -s -S -T - --header %s --retry 12 --user %s:%s %s',
			escapeshellarg('Content-encoding: gzip'), escapeshellarg($username), escapeshellarg($password),
			escapeshellarg('https://' . $url . '/data.csv'));

		$this->_command .= ' | ' . $command;

		$process = new Process($this->_command);
		$process->setTimeout(null);
		$process->run();
		if (!$process->isSuccessful()) {
			$message = 'CSV handling failed. ' . $process->getErrorOutput();
			$e = new CsvHandlerException($message);
			$e->setData(array('command' => $this->_command));
			if (!$process->getErrorOutput()) {
				$e->setData(array(
					'priority' => 'alert',
					'command' => $this->_command,
					'error' => 'No error output'
				));
			}
			throw $e;
		}

		$this->_command = null;
	}



	/**
	 * Create manifest for data load
	 * @param $xmlFileObject
	 * @param $incrementalLoad
	 * @return array
	 */
	public function getManifest($xmlFileObject, $incrementalLoad)
	{
		$datasetName = self::gdName($xmlFileObject->name);
		$manifest = array(
			'dataSetSLIManifest' => array(
				'file' => 'data.csv',
				'dataSet' => 'dataset.' . $datasetName,
				'parts' => array()
			)
		);
		foreach ($xmlFileObject->columns->column as $column) {
			$columnName = self::gdName($column->name);
			$gdName = null;
			switch ((string)$column->ldmType) {
				case 'CONNECTION_POINT':
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name,
						'populates' => array(
							sprintf('label.%s.%s', $datasetName, $columnName)
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
						'referenceKey' => 1
					);
					break;
				case 'FACT':
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name,
						'populates' => array(
							sprintf('fact.%s.%s', $datasetName, $columnName)
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
					);
					break;
				case 'ATTRIBUTE':
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name,
						'populates' => array(
							sprintf('label.%s.%s', $datasetName, $columnName)
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
						'referenceKey' => 1
					);
					break;
				case 'LABEL':
				case 'HYPERLINK':
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name,
						'populates' => array(
							sprintf('label.%s.%s.%s', $datasetName, self::gdName($column->reference), $columnName)
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
					);
					break;
				case 'REFERENCE':
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name,
						'populates' => array(
							sprintf('label.%s.%s', self::gdName($column->schemaReference), self::gdName($column->reference))
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
						'referenceKey' => 1
					);
					break;
				case 'DATE':
					$dimensionName = self::gdName($column->schemaReference);
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name,
						'populates' => array(
							sprintf('%s.date.mmddyyyy', $dimensionName)
						),
						'constraints' => array(
							'date' => (string)$column->format
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
						'referenceKey' => 1
					);
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name . '_dt',
						'populates' => array(
							sprintf('dt.%s.%s', $datasetName, $columnName)
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
					);
					if ((string)$column->datetime == 'true') {
						$manifest['dataSetSLIManifest']['parts'][] = array(
							'columnName' => (string)$column->name . '_tm',
							'populates' => array(
								sprintf('tm.dt.%s.%s', $datasetName, $columnName)
							),
							'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
						);
						$manifest['dataSetSLIManifest']['parts'][] = array(
							'columnName' => (string)$column->name . '_id',
							'populates' => array(
								sprintf('label.time.second.of.day.%s', $dimensionName)
							),
							'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
							'referenceKey' => 1
						);
					}
					break;
				case 'IGNORE':
					break;
			}
		}

		return $manifest;
	}


	/**
	 * Create manifest for data load of time dimension
	 * @param $dimensionName
	 * @return mixed
	 */
	public function getTimeDimensionManifest($dimensionName)
	{
		$manifest = file_get_contents($this->_timeDimensionManifestPath);
		$manifest = str_replace('%NAME%', self::gdName($dimensionName), $manifest);
		return $manifest;
	}


	/**
	 * Download xml to disk
	 * @param $xmlFile
	 * @throws CsvHandlerException
	 * @return string
	 */
	public function downloadXml($xmlFile)
	{
		$xmlUrl = $xmlFile;
		$url = parse_url($xmlFile);
		if (empty($url['host'])) {
			$xmlUrl = $this->_s3Client->url($xmlFile);
		}
		$xmlFilePath = $this->_tmpDir . '/model.xml';

		$command = 'curl -s -L ' . escapeshellarg($xmlUrl) . ' > ' . escapeshellarg($xmlFilePath);
		$process = new Process($command);
		$process->setTimeout(null);
		$process->run();
		if (!$process->isSuccessful()) {
			$e = new CsvHandlerException('XML download failed.');
			$e->setData(array(
				'priority' => 'alert',
				'command' => $command,
				'error' => $process->getErrorOutput()
			));
			throw $e;
		}
		return $xmlFilePath;
	}

	/**
	 * Save and parse definition xml
	 * @param $xmlFile
	 * @return \SimpleXMLElement
	 * @throws CsvHandlerException
	 */
	public function getXml($xmlFile)
	{
		libxml_use_internal_errors(TRUE);
		$xmlFileObject = simplexml_load_file($xmlFile);
		if (!$xmlFileObject) {
			$errors = '';
			foreach (libxml_get_errors() as $error) {
				$errors .= $error->message;
			}
			throw new CsvHandlerException($errors);
		}

		return $xmlFileObject;
	}



	public static function gdName($name)
	{
		$string = iconv('utf-8', 'ascii//ignore//translit', $name);
		$string = preg_replace('/[^\w\d_]/', '', $string);
		$string = preg_replace('/^[\d_]*/', '', $string);
		return strtolower($string);
	}

}