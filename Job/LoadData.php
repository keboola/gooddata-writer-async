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
use Keboola\GoodDataWriter\GoodData\WebDavException;
use Symfony\Component\Process\Process;
use Symfony\Component\Stopwatch\Stopwatch;

class LoadData extends AbstractJob
{
	private $goodDataModel;

	/**
	 * required: pid, tableId
	 * optional: incrementalLoad
	 */
	public function run($job, $params)
	{
		$this->checkParams($params, array('pid', 'tableId'));
		$project = $this->configuration->getProject($params['pid']);
		if (!$project) {
			throw new WrongParametersException($this->translator->trans('parameters.pid_not_configured'));
		}
		if (empty($job['definition'])) {
			throw new WrongConfigurationException($this->translator->trans('job_executor.data_set_definition_missing'));
		}

		$this->configuration->checkBucketAttributes();
		$this->configuration->updateDataSetsFromSapi();

		$this->logEvent('start', array(
			'duration' => 0
		));
		$stopWatch = new Stopwatch();


		// Init
		$bucketAttributes = $this->configuration->bucketAttributes();
		$tmpFolderName = basename($this->tmpDir);
		$this->goodDataModel = new Model($this->appConfiguration);
		$csvHandler = new CsvHandler($this->appConfiguration->scriptsPath, $this->storageApiClient, $this->logger);
		$csvHandler->setJobId($job['id']);
		$csvHandler->setRunId($job['runId']);

		$tableDefinition = $this->configuration->getDataSet($params['tableId']);
		$incrementalLoad = (isset($params['incrementalLoad'])) ? $params['incrementalLoad']
			: (!empty($tableDefinition['incrementalLoad']) ? $tableDefinition['incrementalLoad'] : 0);
		$filterColumn = $this->getFilterColumn($params['tableId'], $tableDefinition, $bucketAttributes);
		$filterColumn = ($filterColumn && empty($project['main'])) ? $filterColumn : false;

		$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);


		// Get definition
		$stopWatchId = 'get_definition';
		$stopWatch->start($stopWatchId);
		$definitionFile = $job['definition'];

		$definitionUrl = $this->s3Client->url($definitionFile);
		$command = 'curl -sS -L --retry 12 ' . escapeshellarg($definitionUrl);
		$process = new Process($command);
		$process->setTimeout(null);
		$process->run();
		$error = $process->getErrorOutput();
		if (!$process->isSuccessful() || $error) {
			throw new \Exception($this->translator->trans('error.s3_download_fail') . ': ' . json_encode(array(
				'command' => $command,
				'error' => $error,
				'output' => $process->getOutput()
			)));
		}
		$definition = json_decode($process->getOutput(), true);
		if (!$definition) {
			throw new \Exception($this->translator->trans('error.s3_download_fail') . ': ' . json_encode(array(
				'command' => $command,
				'error' => $error,
				'output' => $process->getOutput()
			)));
		}

		$e = $stopWatch->stop($stopWatchId);
		$this->logEvent($stopWatchId, array(
			'duration' => $e->getDuration(),
			'definition' => $definitionUrl
		));


		// Get manifest
		$stopWatchId = 'get_manifest';
		$stopWatch->start($stopWatchId);
		$manifest = Model::getDataLoadManifest($definition, $incrementalLoad);
		file_put_contents($this->tmpDir . '/upload_info.json', json_encode($manifest));
		$this->logs['Manifest'] = $this->s3Client->uploadFile($this->tmpDir . '/upload_info.json', 'text/plain', $tmpFolderName . '/manifest.json');
		$e = $stopWatch->stop($stopWatchId);
		$this->logEvent($stopWatchId, array(
			'duration' => $e->getDuration(),
			'manifest' => $this->logs['Manifest']
		));


		try {
			// Upload to WebDav
			$webDav = new WebDav($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

			// Upload dataSets
			$stopWatchId = 'transfer_csv';
			$stopWatch->start($stopWatchId);

			$webDav->prepareFolder($tmpFolderName);

			$webDavFileUrl = sprintf('%s/%s/data.csv', $webDav->getUrl(), $tmpFolderName);
			$csvHandler->runUpload($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password'],
				$webDavFileUrl, $definition, $params['tableId'], $incrementalLoad, $filterColumn, $params['pid']);
			if (!$webDav->fileExists($tmpFolderName . '/data.csv')) {
				throw new JobProcessException($this->translator->trans('error.csv_not_uploaded %1', array('%1' => $webDavFileUrl)));
			}

			$e = $stopWatch->stop($stopWatchId);
			$this->logEvent($stopWatchId, array(
				'duration' => $e->getDuration()
			));

			$stopWatchId = 'upload_manifest';
			$stopWatch->start($stopWatchId);

			$webDav->upload($this->tmpDir . '/upload_info.json', $tmpFolderName);
			$e = $stopWatch->stop($stopWatchId);
			$this->logEvent($stopWatchId, array(
				'duration' => $e->getDuration(),
				'url' => $webDavFileUrl
			));


			// Run ETL tasks
			$gdWriteStartTime = date('c');

			// Run ETL task of dataSets
			$stopWatchId = 'run_etl';
			$stopWatch->start($stopWatchId);
			$this->restApi->initLog();

			try {
				$this->restApi->loadData($params['pid'], $tmpFolderName);
			} catch (RestApiException $e) {
				$debugFile = $this->tmpDir . '/etl.log';
				$taskName = 'Data Load Error';
				$logSaved = $webDav->saveLogs($tmpFolderName, $debugFile);
				if ($logSaved) {
					if (filesize($debugFile) > 1024 * 1024) {
						$logUrl = $this->s3Client->uploadFile($debugFile, 'text/plain', sprintf('%s/etl.log', $tmpFolderName));
						$this->logs[$taskName] = $logUrl;
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
		} catch (\Exception $e) {
			$error = $e->getMessage();
			$event = $stopWatch->stop($stopWatchId);

			$restApiLogPath = null;
			$eventDetails = array(
				'duration' => $event->getDuration()
			);
			if ($e instanceof RestApiException) {
				$error = $e->getDetails();
				$restApiLogPath = $this->restApi->getLogPath();
			}
			$this->logEvent($stopWatchId, $eventDetails, $restApiLogPath);

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
		if (!empty($gdWriteStartTime)) {
			$result['gdWriteStartTime'] = $gdWriteStartTime;
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
				throw new WrongConfigurationException($this->translator->trans('configuration.upload.filter_missing'));
			}
			if (!in_array($filterColumn, $tableInfo['indexedColumns'])) {
				throw new WrongConfigurationException($this->translator->trans('configuration.upload.filter_index_missing'));
			}
		}
		return $filterColumn;
	}

}
