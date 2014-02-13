<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-11
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\JobProcessException;
use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\GoodDataWriter\Writer\AppConfiguration;
use Keboola\GoodDataWriter\Writer\Configuration,
	Keboola\GoodDataWriter\Writer\SharedConfig,
	Keboola\GoodDataWriter\GoodData\RestApi,
	Keboola\GoodDataWriter\Service\S3Client,
	Keboola\StorageApi\Client as StorageApiClient;

abstract class AbstractJob
{
	/**
	 * @var Configuration
	 */
	public $configuration;
	/**
	 * @var AppConfiguration
	 */
	public $appConfiguration;
	/**
	 * @var SharedConfig
	 */
	public $sharedConfig;
	/**
	 * @var StorageApiClient
	 */
	public $storageApiClient;
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
	public $preRelease;
	/**
	 * @var \Monolog\Logger
	 */
	public $log;

	public $eventsLog;

	public function __construct(Configuration $configuration, AppConfiguration $appConfiguration, $sharedConfig, $restApi, $s3Client)
	{
		$this->configuration = $configuration;
		$this->appConfiguration = $appConfiguration;
		$this->sharedConfig = $sharedConfig;
		$this->s3Client = $s3Client;

		$this->restApi = $restApi;
	}


	abstract function run($job, $params);

	/**
	 * @param array $params
	 * @param array $required
	 * @throws WrongConfigurationException
	 */
	protected function checkParams($params, $required)
	{
		foreach($required as $k) {
			if (empty($params[$k])) {
				throw new WrongConfigurationException("Parameter '" . $k . "' is missing");
			}
		}
	}

	protected function getWebDavUrl($bucketAttributes)
	{
		$webDavUrl = null;
		if (isset($bucketAttributes['gd']['backendUrl']) && $bucketAttributes['gd']['backendUrl'] != RestApi::DEFAULT_BACKEND_URL) {

			// Get WebDav url for non-default backend
			$backendUrl = (substr($bucketAttributes['gd']['backendUrl'], 0, 8) != 'https://'
					? 'https://' : '') . $bucketAttributes['gd']['backendUrl'];
			$this->restApi->setBaseUrl($backendUrl);
			$this->restApi->login($this->appConfiguration->gd_username, $this->appConfiguration->gd_password);
			$webDavUrl = $this->restApi->getWebDavUrl();
			if (!$webDavUrl) {
				throw new JobProcessException(sprintf("Getting of WebDav url for backend '%s' failed.", $bucketAttributes['gd']['backendUrl']));
			}
		}
		return $webDavUrl;
	}
}
