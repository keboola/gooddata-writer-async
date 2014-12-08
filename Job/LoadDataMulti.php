<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-17
 */

namespace Keboola\GoodDataWriter\Job;

use Keboola\GoodDataWriter\Exception\ClientException;
use Keboola\GoodDataWriter\Exception\JobProcessException;
use Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException;
use Keboola\GoodDataWriter\Exception\WrongParametersException;
use Keboola\GoodDataWriter\GoodData\CsvHandler,
	Keboola\GoodDataWriter\GoodData\WebDav;
use Keboola\GoodDataWriter\GoodData\Model;
use Keboola\GoodDataWriter\GoodData\RestApi;
use Keboola\GoodDataWriter\GoodData\WebDavException;
use Symfony\Component\Stopwatch\Stopwatch;

class LoadDataMulti extends AbstractJob
{
	private $goodDataModel;

	public function prepare($params)
	{
		$this->checkParams($params, array('writerId', 'tables'));
		$this->checkWriterExistence($params['writerId']);
		$this->configuration->checkBucketAttributes();
		$result = array(
			'tables' => $params['tables']
		);
		if (isset($params['incrementalLoad'])) {
			$result['incrementalLoad'] = $params['incrementalLoad'];
		}
		return $result;
	}

	/**
	 * required: pid, tables
	 */
	public function run($job, $params, RestApi $restApi)
	{
		$this->checkParams($params, array('pid', 'tables'));
		$project = $this->configuration->getProject($params['pid']);
		if (!$project) {
			throw new WrongParametersException($this->translator->trans('parameters.pid_not_configured'));
		}
		if (empty($job['definition'])) {
			throw new WrongConfigurationException($this->translator->trans('job_executor.data_set_definition_missing'));
		}

		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->configuration->checkBucketAttributes($bucketAttributes);

		$stopWatch = new Stopwatch();


		$definition = $this->s3Client->downloadFile($job['definition']);
		$definition = json_decode($definition, true);
		if (!$definition) {
			throw new \Exception($this->translator->trans('error.s3_download_fail') . ': ' . $job['definition']);
		}


		// Init
		$tmpFolderName = basename($this->getTmpDir($job['id']));
		$this->goodDataModel = new Model($this->appConfiguration);
		$csvHandler = new CsvHandler($this->temp, $this->appConfiguration->scriptsPath, $this->storageApiClient, $this->logger);
		$csvHandler->setJobId($job['id']);
		$csvHandler->setRunId($job['runId']);

		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);


		// Get manifest
		$stopWatchId = 'get_manifest';
		$stopWatch->start($stopWatchId);
		$manifest = array();
		foreach ($definition as $tableId => &$def) {
			$def['incrementalLoad'] = !empty($def['dataset']['incrementalLoad']) ? $def['dataset']['incrementalLoad'] : 0;
			$def['filterColumn'] = $this->getFilterColumn($tableId, $def['dataset'], $bucketAttributes);
			$def['filterColumn'] = ($def['filterColumn'] && empty($project['main'])) ? $def['filterColumn'] : false;
			$manifest[] = Model::getDataLoadManifest($def['columns'], $def['incrementalLoad'], $this->configuration->noDateFacts);
		}
		$manifest = array('dataSetSLIManifestList' => $manifest);


		file_put_contents($this->getTmpDir($job['id']) . '/upload_info.json', json_encode($manifest));
		$this->logs['Manifest'] = $this->s3Client->uploadFile($this->getTmpDir($job['id']) . '/upload_info.json', 'text/plain', $tmpFolderName . '/manifest.json');
		$e = $stopWatch->stop($stopWatchId);
		$this->logEvent('Manifest file for csv prepared', $job['id'], $job['runId'], array(
			'manifest' => $this->s3Client->url($this->logs['Manifest'])
		), $e->getDuration());

		try {
			// Upload to WebDav
			$webDav = new WebDav($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);
			$webDav->prepareFolder($tmpFolderName);

			// Upload dataSets
			foreach ($definition as $tableId => $d) {
				$stopWatchId = 'transfer_csv_' . $tableId;
				$stopWatch->start($stopWatchId);

				$datasetName = Model::getId($d['columns']['name']);
				$webDavFileUrl = sprintf('%s/%s/%s.csv', $webDav->getUrl(), $tmpFolderName, $datasetName);
				try {
					$csvHandler->runUpload($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password'],
						$webDavFileUrl, $d['columns'], $tableId, $d['incrementalLoad'], $d['filterColumn'], $params['pid'],
						$this->configuration->noDateFacts);
				} catch (ClientException $e) {
					throw new ClientException(sprintf("Error during upload of dataset '%s': %s", $datasetName, $e->getMessage()), $e);
				}
				if (!$webDav->fileExists(sprintf('%s/%s.csv', $tmpFolderName, $datasetName))) {
					throw new JobProcessException($this->translator->trans('error.csv_not_uploaded %1', array('%1' => $webDavFileUrl)));
				}

				$e = $stopWatch->stop($stopWatchId);
				$this->logEvent('Csv file ' . $datasetName . '.csv transferred to WebDav', $job['id'], $job['runId'], array(
					'url' => $webDavFileUrl
				), $e->getDuration());
			}


			$stopWatchId = 'upload_manifest';
			$stopWatch->start($stopWatchId);

			$webDav->upload($this->getTmpDir($job['id']) . '/upload_info.json', $tmpFolderName);
			$e = $stopWatch->stop($stopWatchId);
			$this->logEvent('Manifest file for csv transferred to WebDav', $job['id'], $job['runId'], array(
				'url' => $webDav->getUrl() . '/' . $tmpFolderName . '/upload_info.json'
			), $e->getDuration());


			// Run ETL task of dataSets
			$stopWatchId = 'run_etl';
			$stopWatch->start($stopWatchId);

			try {
				$restApi->loadData($params['pid'], $tmpFolderName);
			} catch (RestApiException $e) {
				$debugFile = $this->getTmpDir($job['id']) . '/etl.log';
				$logSaved = $webDav->saveLogs($tmpFolderName, $debugFile);
				if ($logSaved) {
					if (filesize($debugFile) > 1024 * 1024) {
						$logUrl = $this->s3Client->uploadFile($debugFile, 'text/plain', sprintf('%s/etl.log', $tmpFolderName));
						$this->logs['ETL task error'] = $logUrl;
						$e->setDetails(array($logUrl));
					} else {
						$e->setDetails(file_get_contents($debugFile));
					}
				}

				throw $e;
			}
			$e = $stopWatch->stop($stopWatchId);
			$this->logEvent('ETL task finished', $job['id'], $job['runId'], array(), $e->getDuration());
		} catch (\Exception $e) {
			$error = $e->getMessage();

			if ($e instanceof RestApiException) {
				$error = $e->getDetails();
			}

			$sw = $stopWatch->isStarted($stopWatchId)? $stopWatch->stop($stopWatchId) : null;
			$this->logEvent('ETL task failed', $job['id'], $job['runId'], array(
				'error' => $error
			), $sw? $sw->getDuration() : 0);

			if (!($e instanceof RestApiException) && !($e instanceof WebDavException)) {
				throw $e;
			}
		}

		$result = array();
		if (!empty($error)) {
			$result['error'] = $error;
		}

		return $result;
	}


	private function getFilterColumn($tableId, $tableDefinition, $bucketAttributes)
	{
		$filterColumn = null;
		if (isset($bucketAttributes['filterColumn']) && empty($tableDefinition['ignoreFilter'])) {
			$filterColumn = $bucketAttributes['filterColumn'];
			$tableInfo = $this->configuration->getSapiTable($tableId);
			if (!in_array($filterColumn, $tableInfo['columns'])) {
				throw new WrongConfigurationException($this->translator->trans('configuration.upload.filter_missing', array('%1' => $filterColumn)));
			}
			if (!in_array($filterColumn, $tableInfo['indexedColumns'])) {
				throw new WrongConfigurationException($this->translator->trans('configuration.upload.filter_index_missing', array('%1' => $filterColumn)));
			}
		}
		return $filterColumn;
	}

}
