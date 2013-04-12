<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;


use Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\GoodDataWriter\Writer\SharedConfig,
	Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\GoodDataWriter\GoodData\CLToolApi;

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
	 * @var \Syrup\ComponentBundle\Monolog\Uploader\SyrupS3Uploader
	 */
	public $logUploader;


	public function __construct($configuration, $mainConfig, $sharedConfig, $restApi, $logUploader)
	{
		$this->configuration = $configuration;
		$this->mainConfig = $mainConfig;
		$this->sharedConfig = $sharedConfig;
		$this->logUploader = $logUploader;

		$this->restApi = $restApi;
	}


	abstract function run($job, $params);


	protected function _prepareResult($jobId, $data = array(), $callsLog = null)
	{
		$logUrl = null;
		if ($callsLog) {
			$logUrl = $this->logUploader->uploadString('calls-' . $jobId, $callsLog);
		}

		if ($logUrl) {
			$data['log'] = $logUrl;
		}

		return $data;
	}
}