<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-08-06
 */

namespace Keboola\GoodDataWriter\GoodData;

use Symfony\Component\Process\Process;
use Keboola\GoodDataWriter\Exception\ClientException,
	Keboola\GoodDataWriter\Exception\JobProcessException;

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
	 */
	public function initDownload($tableId, $token, $sapiUrl, $userAgent, $incrementalLoad = false, $filterColumn = false, $filterValue = null)
	{
		$incrementalLoad = $incrementalLoad ? '&changedSince=-' . $incrementalLoad . '+days' : null;
		$filter = ($filterColumn && $filterValue) ? '&whereColumn=' . $filterColumn . '&whereValues[]=' . $filterValue : null;
		$this->_command = sprintf('curl -g -s --header "Accept-encoding: gzip" --header "X-StorageApi-Token: %s"'
			.' --header "X-KBC-RunId: %d" --user-agent "%s" "%s/v2/storage/tables/%s/export?format=escaped%s%s" | gzip -d',
			$token, $this->_jobId, $userAgent, $sapiUrl, $tableId, $incrementalLoad, $filter);
	}

	/**
	 * Sanitize csv - normalize dates and set proper values for empty facts and attributes
	 * @param $xmlFileObject
	 * @return void
	 */
	public function prepareSanitization($xmlFileObject)
	{
		$nullReplace = 'sed \'s/\"NULL\"/\"\"/g\' | awk -v OFS="\",\"" -F"\",\"" \'{';

		$i = 1;
		$columnsCount = $xmlFileObject->columns->column->count();
		foreach ($xmlFileObject->columns->column as $column) {
			$type = (string)$column->ldmType;
			$value = NULL;
			switch ($type) {
				case 'ATTRIBUTE':
					$value = '-- empty --';
					break;
				case 'LABEL':
				case 'FACT':
					$value = '0';
					break;
				case 'DATE':
					$format = (string)$column->format;
					$value = str_replace(
						array('yyyy', 'MM', 'dd', 'hh', 'HH', 'mm', 'ss', 'kk'),
						array('1900', '01', '01', '00', '00', '00', '00', '00'),
						$format);
					break;
			}
			if (!is_null($value)) {
				$testValue = '""';
				if ($i == 1) {
					$testValue = '"\""';
					$value = '\"' . $value;
				}
				if ($i == $columnsCount) {
					$testValue = '"\""';
					$value .= '\"';
				}
				$nullReplace .= 'if ($' . $i . ' == ' . $testValue . ') {$' . $i . ' = "' . $value . '"} ';
			}
			$i++;
		}
		$nullReplace .= '; print }\'';

		$this->_command .= ' | ' . $nullReplace;
	}

	/**
	 * Parse csv and prepare for data load
	 * @param $xmlFileObject
	 * @throws \Keboola\GoodDataWriter\Exception\JobProcessException
	 */
	public function prepareTransformation($xmlFileObject)
	{
		$csvHeaders = array();
		$ignoredColumnsIndices = array();
		$dateColumnsIndices = array();
		$timeColumnsIndices = array();
		$i = 1;
		foreach ($xmlFileObject->columns->column as $column) {
			$columnName = self::gdName($column->name);
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

	public function runDownload($csvFile)
	{
		if (!$this->_command) {
			throw new CsvHandlerException('You must init the download first');
		}

		$this->_command .= ' > ' . escapeshellarg($csvFile);

		$process = new Process($this->_command);
		$process->setTimeout(null);
		$process->run();
		if (!$process->isSuccessful()) {
			$message = $process->getErrorOutput() ? $process->getErrorOutput() : 'CSV download and preparation failed.';
			$e = new CsvHandlerException($message);
			if (!$process->getErrorOutput()) {
				$e->setData(array(
					'priority' => 'alert',
					'command' => $this->_command,
					'error' => 'No error output'
				));
			}
			throw $e;
		}
		if (!file_exists($csvFile)) {
			$e = new CsvHandlerException('CSV download and preparation failed');
			$e->setData(array(
				'priority' => 'alert',
				'command' => $this->_command,
				'error' => 'Csv not created'
			));
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
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name,
						'populates' => array(
							sprintf('%s.date.mmddyyyy', self::gdName($column->schemaReference))
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
								sprintf('label.time.second.of.day.%s', self::gdName($column->schemaReference))
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