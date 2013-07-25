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
	Keboola\GoodDataWriter\GoodData\CLToolApi,
	Keboola\GoodDataWriter\Service\S3Client;

abstract class GenericJob
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
	 * @var CLToolApi
	 */
	public $clToolApi;
	/**
	 * @var S3Client
	 */
	public $s3Client;
	public $tmpDir;
	public $rootPath;


	public function __construct($configuration, $mainConfig, $sharedConfig, $restApi, $clToolApi, $s3Client)
	{
		$this->configuration = $configuration;
		$this->mainConfig = $mainConfig;
		$this->sharedConfig = $sharedConfig;
		$this->s3Client = $s3Client;

		$this->restApi = $restApi;
		$this->clToolApi = $clToolApi;
	}


	abstract function run($job, $params);


	protected function _prepareResult($jobId, $data = array(), $callsLog = null)
	{
		$logUrl = null;
		if ($callsLog) {
			$logUrl = $this->s3Client->uploadString('calls-' . $jobId, $callsLog);
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
