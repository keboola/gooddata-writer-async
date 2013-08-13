<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;
use Keboola\GoodDataWriter\GoodData\CsvHandler;
use Keboola\GoodDataWriter\GoodData\WebDav;

class CreateDate extends GenericJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @return array
	 */
	public function run($job, $params)
	{
		if (empty($params['name'])) {
			throw new WrongConfigurationException("Parameter 'dataset' is missing");
		}
		if (!isset($params['includeTime'])) {
			throw new WrongConfigurationException("Parameter 'includeTime' is missing");
		}
		if (empty($params['pid'])) {
			throw new WrongConfigurationException("Parameter 'pid' is missing");
		}
		$this->configuration->checkGoodDataSetup();


		$gdWriteStartTime = date('c');
		try {
			$this->restApi->login($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			$this->restApi->createDateDimension($params['pid'], $params['name'], $params['includeTime']);

			$csvHandler = new CsvHandler($this->rootPath);
			$webDav = new WebDav($this->configuration->bucketInfo['gd']['username'], $this->configuration->bucketInfo['gd']['password']);
			$manifest = $csvHandler->getTimeDimensionManifest($params['name']);

			$tmpFolderName = $job['id'] . '-' . uniqid();
			$tmpFolder = $this->tmpDir . '/' . $tmpFolderName;
			mkdir($tmpFolder);

			file_put_contents($tmpFolder . '/upload_info.json', $manifest);
			copy($this->rootPath . '/GoodData/time-dimension.csv', $tmpFolder . '/data.csv');
			$csvFileSize = filesize($tmpFolder . '/data.csv');

			$debug = array();
			$result = $this->restApi->loadData($params['pid'], $tmpFolderName);
			if ($result['taskStatus'] == 'ERROR' || $result['taskStatus'] == 'WARNING') {

				$debugFile = $tmpFolder . '/data-load-log.txt';

				// Find upload message
				$uploadMessage = $this->restApi->getUploadMessage($params['pid'], 'time.' . $csvHandler->gdName($params['name']));
				if ($uploadMessage) {
					file_put_contents($debugFile, $uploadMessage . PHP_EOL . PHP_EOL, FILE_APPEND);
				}

				// Look for .json and .log files in WebDav folder
				$webDav->getLogs($tmpFolderName, $debugFile);
				$debug['loadData'] = $this->s3Client->uploadFile($debugFile);

			}

			return $this->_prepareResult($job['id'], array(
				'gdWriteStartTime' => $gdWriteStartTime,
				'gdWriteBytes' => $csvFileSize,
				'debug' => $debug
			), $this->restApi->callsLog());

		} catch (UnauthorizedException $e) {
			throw new WrongConfigurationException('Rest API Login failed');
		} catch (RestApiException $e) {
			return $this->_prepareResult($job['id'], array(
				'status' => 'error',
				'error' => $e->getMessage(),
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());
		}

	}
}