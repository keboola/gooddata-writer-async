<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 10.02.14
 * Time: 10:54
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\GoodData\WebDav;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\WebDavException;
use Symfony\Component\Stopwatch\Stopwatch;

class UploadDateDimension extends AbstractJob
{
	/**
	 * @var Model
	 */
	private $goodDataModel;
	public $eventsLog;

	public function run($job, $params)
	{
		$this->checkParams($params, array('name'));

		$this->logEvent('start', array(
			'duration' => 0
		));
		$stopWatch = new Stopwatch();
		$stopWatchId = 'prepareJob';
		$stopWatch->start($stopWatchId);

		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->configuration->checkBucketAttributes();

		$dateDimensions = $this->configuration->getDateDimensions();
		if (!in_array($params['name'], array_keys($dateDimensions))) {
			throw new WrongConfigurationException(sprintf("Date dimension '%s' does not exist in configuration", $params['name']));
		}

		// Init
		$tmpFolderName = basename($this->tmpDir);
		$this->goodDataModel = new Model($this->appConfiguration);
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		// Choose projects to load
		$projectsToLoad = array();
		foreach ($this->configuration->getProjects() as $project) if ($project['active']) {
			if (in_array($project['pid'], $projectsToLoad)) {
				throw new WrongConfigurationException("Project '" . $project['pid'] . "' is duplicated in configuration");
			}

			if (!isset($params['pid']) || $project['pid'] == $params['pid']) {
				$projectsToLoad[] = $project['pid'];
			}
		}
		if (isset($params['pid']) && !count($projectsToLoad)) {
			throw new WrongConfigurationException("Project '" . $params['pid'] . "' was not found in configuration");
		}

		$includeTime = $dateDimensions[$params['name']]['includeTime'];

		$e = $stopWatch->stop($stopWatchId);
		$this->logEvent($stopWatchId, array(
			'duration' => $e->getDuration()
		), $this->restApi->getLogPath());
		$this->restApi->initLog();


		try {
			// Create date dimensions
			foreach ($projectsToLoad as $pid) {
				$stopWatchId = 'createDimension-' . $params['name'] . '-' . $pid;
				$stopWatch->start($stopWatchId);
				$this->restApi->initLog();

				$this->restApi->createDateDimension($pid, $params['name'], $includeTime);

				$e = $stopWatch->stop($stopWatchId);
				$this->logEvent($stopWatchId, array(
					'duration' => $e->getDuration()
				), $this->restApi->getLogPath());
			}

			if ($includeTime) {
				// Upload to WebDav
				$webDavUrl = $this->getWebDavUrl($bucketAttributes);
				$webDav = new WebDav($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password'], $webDavUrl);

				// Upload time dimension data
				$stopWatchId = 'uploadTimeDimension-' . $params['name'];
				$stopWatch->start($stopWatchId);

				$dimensionName = Model::getId($params['name']);
				$tmpFolderDimension = $this->tmpDir . '/' . $dimensionName;
				$tmpFolderNameDimension = $tmpFolderName . '-' . $dimensionName;

				mkdir($tmpFolderDimension);
				$timeDimensionManifest = $this->goodDataModel->getTimeDimensionDataLoadManifest($params['name']);
				file_put_contents($tmpFolderDimension . '/upload_info.json', $timeDimensionManifest);
				copy($this->scriptsPath . '/time-dimension.csv', $tmpFolderDimension . '/data.csv');
				$webDav->prepareFolder($tmpFolderNameDimension);
				$webDav->upload($tmpFolderDimension . '/upload_info.json', $tmpFolderNameDimension);
				$webDav->upload($tmpFolderDimension . '/data.csv', $tmpFolderNameDimension);
				$dimensionsToUpload[] = $params['name'];

				$e = $stopWatch->stop($stopWatchId);
				$this->logEvent($stopWatchId, array(
					'duration' => $e->getDuration(),
					'url' => $webDav->url,
					'folder' => '/uploads/' . $tmpFolderNameDimension)
				);


				// Run ETL task of time dimensions
				$gdWriteStartTime = date('c');
				foreach ($projectsToLoad as $pid) {
					$stopWatchId = sprintf('runEtlTimeDimension-%s-%s', $params['name'], $pid);
					$stopWatch->start($stopWatchId);
					$this->restApi->initLog();

					$dataSetName = 'time.' . $dimensionName;
					try {
						$this->restApi->loadData($pid, $tmpFolderNameDimension);
					} catch (RestApiException $e) {
						$debugFile = $tmpFolderDimension . '/' . $pid . '-etl.log';
						$taskName = 'Data Load Error';
						$logSaved = $webDav->saveLogs($tmpFolderDimension, $debugFile);
						if ($logSaved) {
							if (filesize($debugFile) > 1024 * 1024) {
								$logUrl = $this->s3Client->uploadFile($debugFile, 'text/plain', sprintf('%s/%s/%s-etl.log', $tmpFolderName, $pid, $dataSetName));
								if ($pid == $bucketAttributes['gd']['pid']) {
									$this->logs[$taskName] = $logUrl;
								} else {
									$this->logs[$pid][$taskName] = $logUrl;
								}
								$e->setDetails(array($logUrl));
							} else {
								$e->setDetails(file_get_contents($debugFile));
							}
						}

						throw $e;
					}

					$e = $stopWatch->stop($stopWatchId);
					$this->logEvent($stopWatchId, array(
						'duration' => $e->getDuration()
					), $this->restApi->getLogPath());
				}
			}

		} catch (\Exception $e) {
			$error = $e->getMessage();
			$event = $stopWatch->stop($stopWatchId);

			$restApiLogPath = null;
			$eventDetail = array(
				'duration' => $event->getDuration()
			);
			if ($e instanceof RestApiException) {
				$error = $e->getDetails();
				$restApiLogPath = $this->restApi->getLogPath();
			}
			$this->logEvent($stopWatchId, $eventDetail, $restApiLogPath);

			if (!($e instanceof RestApiException) && !($e instanceof WebDavException)) {
				throw $e;
			}
		}

		$result = array();
		if (empty($error)) {
			$this->configuration->setDateDimensionIsExported($params['name']);
		} else {
			$result['error'] = $error;
		}
		if (!empty($gdWriteStartTime)) {
			$result['gdWriteStartTime'] = $gdWriteStartTime;
		}

		return $result;
	}


} 