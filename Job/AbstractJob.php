<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\GoodDataWriter\Writer\SharedConfig,
	Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\GoodDataWriter\Service\S3Client;

abstract class AbstractJob
{
	/**
	 * @var Configuration
	 */
	public $configuration;
	/**
	 * @var array
	 */
	public $mainConfig;
	/**
	 * @var SharedConfig
	 */
	public $sharedConfig;
	/**
	 * @var RestApi
	 */
	public $restApi;
	/**
	 * @var S3Client
	 */
	public $s3Client;
	public $tmpDir;
	public $rootPath;
	public $scriptsPath;
	/**
	 * @var \Monolog\Logger
	 */
	public $log;


	public function __construct($configuration, $mainConfig, $sharedConfig, $restApi, $s3Client)
	{
		$this->configuration = $configuration;
		$this->mainConfig = $mainConfig;
		$this->sharedConfig = $sharedConfig;
		$this->s3Client = $s3Client;

		$this->restApi = $restApi;
	}


	abstract function run($job, $params);


	protected function _prepareResult($jobId, $data = array(), $log = null, $folderName = null)
	{
		$logUrl = null;
		if ($log) {
			if (!defined('JSON_PRETTY_PRINT')) {
				// fallback for PHP <= 5.3
				define('JSON_PRETTY_PRINT', 0);
			}
			$fileName = ($folderName ? $folderName : $jobId) . '/' . 'log.json';
			$logUrl = $this->s3Client->uploadString($fileName, json_encode($log, JSON_PRETTY_PRINT));
		}

		if ($logUrl) {
			$data['log'] = $logUrl;
		}

		return $data;
	}

	/**
	 * @param array $params
	 * @param array $required
	 * @throws WrongConfigurationException
	 */
	protected function _checkParams($params, $required)
	{
		foreach($required as $k) {
			if (empty($params[$k])) {
				throw new WrongConfigurationException("Parameter '" . $k . "' is missing");
			}
		}
	}
}
