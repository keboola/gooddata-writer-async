<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-08-06
 */

namespace Keboola\GoodDataWriter\GoodData;

use Keboola\GoodDataWriter\Exception\JobProcessException,
	Keboola\GoodDataWriter\Service\ProcessException;
use Keboola\GoodDataWriter\Service\Process;

class CsvHandler
{

	private $_timeDimensionManifestPath;
	private $_scriptPath;
	private $_tmpDir;

	/**
	 * @var \Keboola\GoodDataWriter\Service\S3Client
	 */
	private $_s3Client;


	/**
	 * @param $rootPath
	 * @param $s3Client
	 * @param $tmpDir
	 */
	public function __construct($rootPath, $s3Client, $tmpDir)
	{
		$this->_scriptPath = $rootPath . '/GoodData/convert_csv.php';
		$this->_timeDimensionManifestPath = $rootPath . '/GoodData/time-dimension-manifest.json';
		$this->_s3Client = $s3Client;
		$this->_tmpDir = $tmpDir;
	}


	/**
	 * Sanitize csv - normalize dates and set proper values for empty facts and attributes
	 * @param $xmlFileObject
	 * @param $csvFile
	 * @throws \Keboola\GoodDataWriter\Exception\JobProcessException
	 */
	public function sanitize($xmlFileObject, $csvFile)
	{
		rename($csvFile, $csvFile . '.in');
		$nullReplace = 'cat ' . escapeshellarg($csvFile . '.in') . ' | sed \'s/\"NULL\"/\"\"/g\' | awk -v OFS="\",\"" -F"\",\"" \'{';

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
		$nullReplace .= '; print }\' > ' . escapeshellarg($csvFile);
		shell_exec($nullReplace);
		if (!file_exists($csvFile . '.in')) {
			throw new JobProcessException(sprintf("CSV sanitization failed. Job id is '%s'", basename(dirname($csvFile))));
		}
		unlink($csvFile . '.in');
	}


	/**
	 * Create manifest for data load
	 * @param $xmlFileObject
	 * @param $incrementalLoad
	 * @return array
	 */
	public function getManifest($xmlFileObject, $incrementalLoad)
	{
		$datasetName = $this->gdName($xmlFileObject->name);
		$manifest = array(
			'dataSetSLIManifest' => array(
				'file' => 'data.csv',
				'dataSet' => 'dataset.' . $datasetName,
				'parts' => array()
			)
		);
		foreach ($xmlFileObject->columns->column as $column) {
			$columnName = $this->gdName($column->name);
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
							sprintf('label.%s.%s.%s', $datasetName, $this->gdName($column->reference), $columnName)
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
					);
					break;
				case 'REFERENCE':
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name,
						'populates' => array(
							sprintf('label.%s.%s', $this->gdName($column->schemaReference), $this->gdName($column->reference))
						),
						'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
						'referenceKey' => 1
					);
					break;
				case 'DATE':
					$manifest['dataSetSLIManifest']['parts'][] = array(
						'columnName' => (string)$column->name,
						'populates' => array(
							sprintf('%s.date.mmddyyyy', $this->gdName($column->schemaReference))
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
								sprintf('label.time.second.of.day.%s', $this->gdName($column->schemaReference))
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
		$manifest = str_replace('%NAME%', $this->gdName($dimensionName), $manifest);
		return $manifest;
	}


	/**
	 * Download xml to disk
	 * @param $xmlFile
	 * @throws \Keboola\GoodDataWriter\Exception\JobProcessException
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
		try {
			$output = Process::exec('curl -s -L ' . escapeshellarg($xmlUrl) . ' > ' . escapeshellarg($xmlFilePath));
		} catch (ProcessException $e) {
			throw new JobProcessException(sprintf("XML download failed: %s", $e->getMessage()), NULL, $e);
		}
		return $xmlFilePath;
	}

	/**
	 * Save and parse definition xml
	 * @param $xmlFile
	 * @return \SimpleXMLElement
	 * @throws \Keboola\GoodDataWriter\Exception\JobProcessException
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
			throw new JobProcessException('XML load error: ' . $errors);
		}

		return $xmlFileObject;
	}


	/**
	 * Parse csv and prepare for data load
	 * @param $xmlFileObject
	 * @param $csvFile
	 * @throws \Keboola\GoodDataWriter\Exception\JobProcessException
	 */
	public function prepareCsv($xmlFileObject, $csvFile)
	{
		$csvHeaders = array();
		$ignoredColumnsIndices = array();
		$dateColumnsIndices = array();
		$timeColumnsIndices = array();
		$i = 1;
		foreach ($xmlFileObject->columns->column as $column) {
			$columnName = $this->gdName($column->name);
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
		rename($csvFile, $csvFile . '.1');
		$command  = 'cat ' . escapeshellarg($csvFile . '.1') . ' | php ' . escapeshellarg($this->_scriptPath);
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
		$command .= ' > ' . escapeshellarg($csvFile);
		try {
			$output = Process::exec($command);
		} catch (ProcessException $e) {
			throw new JobProcessException(sprintf("CSV preparation failed: %s", $e->getMessage()), NULL, $e);
		}
		if (!file_exists($csvFile)) {
			throw new JobProcessException(sprintf("CSV preparation failed. Job id is '%s'", basename($this->_tmpDir)));
		}
		unlink($csvFile . '.1');
	}


	public function gdName($name)
	{
		return mb_strtolower(str_replace(' ', '', (string)$name));
	}

}