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
use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\GoodData\RestApi;
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

	public function prepare($params)
	{
		$this->checkParams($params, array('writerId', 'tableId', 'name'));
		$this->checkWriterExistence($params['writerId']);
		$this->configuration->checkBucketAttributes();

		$dateDimensions = $this->configuration->getDateDimensions();
		if (!in_array($params['name'], array_keys($dateDimensions))) {
			throw new WrongParametersException($this->translator->trans('parameters.dimension_name'));
		}

		return array(
			'name' => $params['name'],
			'includeTime' => $dateDimensions[$params['name']]['includeTime']
		);
	}

	/**
	 * required: pid, name
	 * optional:
	 */
	public function run($job, $params, RestApi $restApi)
	{
		$this->checkParams($params, array('pid', 'name'));
		$project = $this->configuration->getProject($params['pid']);
		if (!$project) {
			throw new WrongParametersException($this->translator->trans('parameters.pid_not_configured'));
		}

		$stopWatch = new Stopwatch();

		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->configuration->checkBucketAttributes($bucketAttributes);

		$dateDimensions = $this->configuration->getDateDimensions();
		if (!in_array($params['name'], array_keys($dateDimensions))) {
			throw new WrongConfigurationException($this->translator->trans('parameters.dimension_name'));
		}

		// Init
		$tmpFolderName = basename($this->getTmpDir($job['id']));
		$this->goodDataModel = new Model($this->appConfiguration);
		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

		$includeTime = $dateDimensions[$params['name']]['includeTime'];
		$template = $dateDimensions[$params['name']]['template'];

		$stopWatchId = 'createDimension-' . $params['name'];
		$stopWatch->start($stopWatchId);

		try {
			// Create date dimensions
			$restApi->createDateDimension($params['pid'], $params['name'], $includeTime, $template);

			$e = $stopWatch->stop($stopWatchId);
			$this->logEvent('Rest API called', $job['id'], $job['runId'], array(), $e->getDuration());

			if ($includeTime) {
				// Upload to WebDav
				$webDav = new WebDav($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

				// Upload time dimension data
				$stopWatchId = 'uploadTimeDimension-' . $params['name'];
				$stopWatch->start($stopWatchId);

				$dimensionName = Model::getId($params['name']);
				$tmpFolderDimension = $this->getTmpDir($job['id']) . '/' . $dimensionName;
				$tmpFolderNameDimension = $tmpFolderName . '-' . $dimensionName;

				mkdir($tmpFolderDimension);
				$timeDimensionManifest = $this->goodDataModel->getTimeDimensionDataLoadManifest($params['name']);
				file_put_contents($tmpFolderDimension . '/upload_info.json', $timeDimensionManifest);
				copy($this->appConfiguration->scriptsPath . '/time-dimension.csv', $tmpFolderDimension . '/' . $dimensionName . '.csv');
				$webDav->prepareFolder($tmpFolderNameDimension);
				$webDav->upload($tmpFolderDimension . '/upload_info.json', $tmpFolderNameDimension);
				$webDav->upload($tmpFolderDimension . '/' . $dimensionName . '.csv', $tmpFolderNameDimension);
				$dimensionsToUpload[] = $params['name'];

				$e = $stopWatch->stop($stopWatchId);
				$this->logEvent('Time dimension data uploaded to WebDav', $job['id'], $job['runId'], array(
					'destination' => $webDav->getUrl() . '/uploads/' . $tmpFolderNameDimension
				), $e->getDuration());

				// Run ETL task of time dimensions
				$stopWatchId = sprintf('runEtlTimeDimension-%s', $params['name']);
				$stopWatch->start($stopWatchId);

				$dataSetName = 'time.' . $dimensionName;
				try {
					$restApi->loadData($params['pid'], $tmpFolderNameDimension);
				} catch (RestApiException $e) {
					$debugFile = $tmpFolderDimension . '/' . $params['pid'] . '-etl.log';
					$logSaved = $webDav->saveLogs($tmpFolderDimension, $debugFile);
					if ($logSaved) {
						if (filesize($debugFile) > 1024 * 1024) {
							$logUrl = $this->s3Client->uploadFile($debugFile, 'text/plain', sprintf('%s/%s/%s-etl.log', $tmpFolderName, $params['pid'], $dataSetName));
							$this->logs['ETL task error'] = $logUrl;
							$e->setDetails(array($logUrl));
						} else {
							$e->setDetails(file_get_contents($debugFile));
						}
					}

					throw $e;
				}

				$e = $stopWatch->stop($stopWatchId);
				$this->logEvent('Time dimension ETL task finished', $job['id'], $job['runId'], array(), $e->getDuration());
			}

		} catch (\Exception $e) {
			$error = $e->getMessage();
			$event = $stopWatch->stop($stopWatchId);

			if ($e instanceof RestApiException) {
				$error = $e->getDetails();
			}
			$this->logEvent('Time dimension ETL task failed', $job['id'], $job['runId'], array(), $event->getDuration());

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

		return $result;
	}


} 