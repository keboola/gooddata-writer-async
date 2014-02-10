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
use Keboola\GoodDataWriter\GoodData\CsvHandler,
	Keboola\GoodDataWriter\GoodData\WebDav;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\WebDavException;
use Symfony\Component\Stopwatch\Stopwatch;

class UploadDateDimension extends AbstractJob
{
	/**
	 * @var CsvHandler
	 */
	private $goodDataModel;
	public $eventsLog;

	public function run($job, $params)
	{
		$this->checkParams($params, array('name', 'includeTime'));

		$this->eventsLog = array();
		$this->eventsLog['start'] = array('duration' => 0, 'time' => date('c'));
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
		$debug = array();
		$tmpFolderName = basename($this->tmpDir);
		$this->goodDataModel = new Model($this->appConfiguration);
		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		// Choose projects to load
		$projectsToLoad = array();
		foreach ($this->configuration->getProjects() as $project) if ($project['active']) {
			if (in_array($project['pid'], array_keys($projectsToLoad))) {
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
		$this->eventsLog[$stopWatchId] = array(
			'duration' => $e->getDuration(),
			'time' => date('c'),
			'restApi' => $this->restApi->callsLog
		);
		$this->restApi->callsLog = array();


		try {
			// Create date dimensions
			foreach ($projectsToLoad as $pid) {
				$stopWatchId = 'createDimension-' . $params['name'] . '-' . $pid;
				$stopWatch->start($stopWatchId);
				$this->restApi->callsLog = array();

				$this->restApi->createDateDimension($pid, $params['name'], $includeTime);

				$e = $stopWatch->stop($stopWatchId);
				$this->eventsLog[$stopWatchId] = array(
					'duration' => $e->getDuration(),
					'time' => date('c'),
					'restApi' => $this->restApi->callsLog
				);
			}

			$this->configuration->setDateDimensionIsExported($params['name']);

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
				$this->eventsLog[$stopWatchId] = array('duration' => $e->getDuration(), 'time' => date('c'),
					'url' => $webDav->url, 'folder' => '/uploads/' . $tmpFolderNameDimension);


				// Run ETL task of time dimensions
				$gdWriteStartTime = date('c');
				foreach ($projectsToLoad as $pid) {
					$stopWatchId = sprintf('runEtlTimeDimension-%s-%s', $params['name'], $pid);
					$stopWatch->start($stopWatchId);

					$dataSetName = 'time.' . $dimensionName;
					try {
						$this->restApi->loadData($pid, $tmpFolderNameDimension, $dataSetName);
					} catch (RestApiException $e) {
						$debugFile = $tmpFolderDimension . '/' . $pid . '-etl.log';
						$taskName = 'Dimension ' . $params['name'];
						$webDav->saveLogs($tmpFolderDimension, $debugFile);
						$logUrl = $this->s3Client->uploadFile($debugFile, 'text/plain', sprintf('%s/%s/%s-etl.log', $tmpFolderName, $pid, $dataSetName));
						$debug[$taskName] = $logUrl;

						throw new RestApiException('ETL load failed', $e->getMessage());
					}

					$e = $stopWatch->stop($stopWatchId);
					$this->eventsLog[$stopWatchId] = array(
						'duration' => $e->getDuration(),
						'time' => date('c'),
						'restApi' => $this->restApi->callsLog
					);
				}
			}

		} catch (\Exception $e) {
			$error = $e->getMessage();
			$event = $stopWatch->stop($stopWatchId);
			$this->eventsLog[$stopWatchId] = array(
				'duration' => $event->getDuration(),
				'time' => date('c')
			);

			if ($e instanceof RestApiException) {
				$error = $e->getDetails();
				$this->eventsLog[$stopWatchId]['restApi'] = $this->restApi->callsLog;
			} elseif ($e instanceof WebDavException) {
				// Do nothing
			} else {
				throw $e;
			}
		}

		$result = array(
			'status' => !empty($error) ? 'error' : 'success',
			'debug' => json_encode($debug)
		);
		if (!empty($error)) {
			$result['error'] = $error;
		}
		if (!empty($gdWriteStartTime)) {
			$result['gdWriteStartTime'] = $gdWriteStartTime;
		}

		return $result;
	}


} 