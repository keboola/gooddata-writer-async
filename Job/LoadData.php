<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\CLToolApiErrorException;

class LoadData extends GenericJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		if (empty($params['pid'])) {
			throw new WrongConfigurationException("Parameter 'pid' is missing");
		}
		if (empty($job['xmlFile'])) {
			throw new WrongConfigurationException("Parameter 'xmlFile' is missing");
		}
		if (!isset($params['incremental'])) {
			throw new WrongConfigurationException("Parameter 'incremental' is missing");
		}
		$this->configuration->checkGoodDataSetup();
		$tableInfo = $this->configuration->getTable($params['tableId']);

		$xmlFile = $job['xmlFile'];
		if (!is_file($xmlFile)) {
			$xmlFilePath = tempnam($this->tmpDir, 'xml');
			exec('curl -s ' . escapeshellarg($xmlFile) . ' > ' . escapeshellarg($xmlFilePath));
			$xmlFile = $xmlFilePath;
		}

		$incrementalLoad = !empty($params['incremental']) ? $params['incremental']
			: (isset($tableInfo['sanitize']) ? $tableInfo['sanitize'] : null);

		$sapiClient = new \Keboola\StorageApi\Client(
			$job['token'],
			$this->mainConfig['storageApi.url'],
			$this->mainConfig['user_agent']
		);
		$csvFile = $this->tmpDir . '/' . $job['id'] . '-' . uniqid() . '.csv';
		$options = array('format' => 'escaped');
		if ($incrementalLoad) {
			$options['changedSince'] = '-' . $incrementalLoad . '+days';
		}
		$sapiClient->exportTable($params['tableId'], $csvFile, $options);


		$sanitize = !empty($params['sanitize']) ? $params['sanitize']
			: (isset($tableInfo['sanitize']) ? $tableInfo['sanitize'] : null);
		libxml_use_internal_errors(TRUE);
		$sxml = simplexml_load_file($xmlFile);
		if ($sxml) {

			if ($sanitize) {
				$nullReplace = 'cat ' . $csvFile . ' | sed \'s/\"NULL\"/\"\"/g\' | awk -v OFS="\",\"" -F"\",\"" \'{';

				$i = 1;
				$columnsCount = $sxml->columns->column->count();
				foreach ($sxml->columns->column as $column) {
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
				$nullReplace .= '; print }\' > ' . $csvFile . '.out';
				shell_exec($nullReplace);

				$csvFile .= '.out';
			}

		} else {
			$errors = '';
			foreach (libxml_get_errors() as $error) {
				$errors .= $error->message;
			}
			return $this->_prepareResult($job['id'], array(
				'status' => 'error',
				'error' => $errors,
				'debug' => $this->clToolApi->debugLogUrl,
				'csvFile' => $csvFile
			), $this->clToolApi->output);
		}


		$gdWriteStartTime = date('c');
		try {
			$this->clToolApi->setCredentials($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			$this->clToolApi->loadData($params['pid'], $xmlFile, $csvFile, $params['incremental']);

			return $this->_prepareResult($job['id'], array(
				'debug' => $this->clToolApi->debugLogUrl,
				'gdWriteStartTime' => $gdWriteStartTime,
				'csvFile' => $csvFile
			), $this->clToolApi->output);

		} catch (CLToolApiErrorException $e) {
			return $this->_prepareResult($job['id'], array(
				'status' => 'error',
				'error' => $e->getMessage(),
				'debug' => $this->clToolApi->debugLogUrl,
				'gdWriteStartTime' => $gdWriteStartTime,
				'csvFile' => $csvFile
			), $this->clToolApi->output);
		}

	}
}