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
	/**
	 * @var AbstractJob
	 */
	protected $_parentJob;

	public function __construct($configuration, $mainConfig, $sharedConfig, $restApi, $s3Client)
	{
		$this->configuration = $configuration;
		$this->mainConfig = $mainConfig;
		$this->sharedConfig = $sharedConfig;
		$this->s3Client = $s3Client;

		$this->restApi = $restApi;
	}


	abstract function run($job, $params);


	protected function _prepareResult($jobId, $data = array(), $callsLog = null, $folderName = null)
	{
		$logUrl = null;
		if ($callsLog) {
			$fileName = ($folderName ? $folderName : $jobId) . '/' . 'api-calls.log';
			$logUrl = $this->s3Client->uploadString($fileName, $callsLog);
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

	/**
	 * @param $command
	 * @return mixed
	 * @throws \Keboola\GoodDataWriter\Exception\WrongConfigurationException
	 */
	protected function _createChildJob($command)
	{
		$commandName = ucfirst($command);
		$commandClass = 'Keboola\GoodDataWriter\Job\\' . $commandName;
		if (!class_exists($commandClass)) {
			throw new WrongConfigurationException(sprintf('Command %s does not exist', $commandName));
		}

		$command = new $commandClass($this->configuration, $this->mainConfig, $this->sharedConfig, $this->restApi, $this->s3Client);
		$command->_setParentJob($this);

		return $command;
	}

	/**
	 * Parent JOB setter
	 *
	 * @param AbstractJob $job
	 * @return $this
	 */
	protected function _setParentJob(AbstractJob $job)
	{
		$this->_parentJob = $job;

		return $this;
	}
}
