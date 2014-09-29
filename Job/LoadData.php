<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-17
 */

namespace Keboola\GoodDataWriter\Job;

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

class LoadData extends AbstractJob
{
	private $goodDataModel;

	public function prepare($params)
	{
		$this->checkParams($params, array('writerId', 'tables'));
		if (!is_array($params['tables'])) {
			throw new JobProcessException($this->translator->trans('parameters.tables_not_array'));
		}
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
	 * required: pid, tableId
	 * optional: incrementalLoad
	 */
	public function run($job, $params, RestApi $restApi)
	{
		$this->checkParams($params, array('pid', 'tableId'));
		$project = $this->configuration->getProject($params['pid']);
		if (!$project) {
			throw new WrongParametersException($this->translator->trans('parameters.pid_not_configured'));
		}
		if (empty($job['definition'])) {
			throw new WrongConfigurationException($this->translator->trans('job_executor.data_set_definition_missing'));
		}

		$bucketAttributes = $this->configuration->bucketAttributes();
		$this->configuration->checkBucketAttributes($bucketAttributes);
		$this->configuration->updateDataSetsFromSapi();

		$stopWatch = new Stopwatch();

		// Init
		$tmpFolderName = basename($this->getTmpDir($job['id']));
		$this->goodDataModel = new Model($this->appConfiguration);
		$csvHandler = new CsvHandler($this->tempService, $this->appConfiguration->scriptsPath, $this->storageApiClient, $this->logger);
		$csvHandler->setJobId($job['id']);
		$csvHandler->setRunId($job['runId']);

		$tableDefinition = $this->configuration->getDataSet($params['tableId']);
		$incrementalLoad = (isset($params['incrementalLoad'])) ? $params['incrementalLoad']
			: (!empty($tableDefinition['incrementalLoad']) ? $tableDefinition['incrementalLoad'] : 0);
		$filterColumn = $this->getFilterColumn($params['tableId'], $tableDefinition, $bucketAttributes);
		$filterColumn = ($filterColumn && empty($project['main'])) ? $filterColumn : false;

		$restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);


		// Get definition
		$stopWatchId = 'get_definition';
		$stopWatch->start($stopWatchId);
		$definitionFile = $job['definition'];

		$definition = $this->s3Client->downloadFile($definitionFile);
		$definition = json_decode($definition, true);
		if (!$definition) {
			throw new \Exception($this->translator->trans('error.s3_download_fail') . ': ' . $definitionFile);
		}

		$e = $stopWatch->stop($stopWatchId);
		$this->logEvent('Data set definition from S3 downloaded', $job['id'], $job['runId'], array(
			'definition' => $definitionFile
		), $e->getDuration());


		// Get manifest
		$stopWatchId = 'get_manifest';
		$stopWatch->start($stopWatchId);
		$manifest = Model::getDataLoadManifest($definition, $incrementalLoad, $this->configuration->noDateFacts);
		file_put_contents($this->getTmpDir($job['id']) . '/upload_info.json', json_encode($manifest));
		$this->logs['Manifest'] = $this->s3Client->uploadFile($this->getTmpDir($job['id']) . '/upload_info.json', 'text/plain', $tmpFolderName . '/manifest.json');
		$e = $stopWatch->stop($stopWatchId);
		$this->logEvent('Manifest file for csv prepared', $job['id'], $job['runId'], array(
			'manifest' => $this->s3Client->url($this->logs['Manifest'])
		), $e->getDuration());


		try {
			// Upload to WebDav
			$webDav = new WebDav($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

			// Upload dataSets
			$stopWatchId = 'transfer_csv';
			$stopWatch->start($stopWatchId);

			$webDav->prepareFolder($tmpFolderName);

			$datasetName = Model::getId($definition['name']);
			$webDavFileUrl = sprintf('%s/%s/%s.csv', $webDav->getUrl(), $tmpFolderName, $datasetName);
			$csvHandler->runUpload($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password'],
				$webDavFileUrl, $definition, $params['tableId'], $incrementalLoad, $filterColumn, $params['pid'],
				$this->configuration->noDateFacts);
			if (!$webDav->fileExists(sprintf('%s/%s.csv', $tmpFolderName, $datasetName))) {
				throw new JobProcessException($this->translator->trans('error.csv_not_uploaded %1', array('%1' => $webDavFileUrl)));
			}

			$e = $stopWatch->stop($stopWatchId);
			$this->logEvent('Csv file transferred to WebDav', $job['id'], $job['runId'], array(
				'url' => $webDavFileUrl
			), $e->getDuration());

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

		$result = array(
			'incrementalLoad' => (int) $incrementalLoad
		);
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
