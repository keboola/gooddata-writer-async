<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-04-12
 */

namespace Keboola\GoodDataWriter\Job;

use Aws\Common\Exception\MultipartUploadException;
use Aws\S3\Model\MultipartUpload\AbstractTransfer;
use Aws\S3\Model\MultipartUpload\UploadBuilder;
use Aws\S3\S3Client;
use Keboola\Csv\CsvFile;
use Keboola\GoodDataWriter\Exception\WrongParametersException,
	Keboola\GoodDataWriter\Exception\WrongConfigurationException,
	Keboola\GoodDataWriter\GoodData\RestApiException,
	Keboola\GoodDataWriter\GoodData\UnauthorizedException;
use Aws\Common\Client as AwsClient;
use Keboola\StorageApi\Table;

class ExportReport extends AbstractJob
{
	/**
	 * @param $job
	 * @param $params
	 * @throws WrongConfigurationException
	 * @throws WrongParametersException
	 * @return array
	 */
	public function run($job, $params)
	{
		$this->configuration->checkBucketAttributes();
		$this->configuration->checkProjectsTable();

		$gdWriteStartTime = date('c');

		try {
			$bucketAttributes = $this->configuration->bucketAttributes();
			$this->restApi->login($bucketAttributes['gd']['username'], $bucketAttributes['gd']['password']);

			$report = $this->restApi->get($params['report']);

			if (!isset($report['report']['content']['definitions'][0])) {
				throw new RestApiException("Report definition not found");
			}
			$reportDefinitions = $report['report']['content']['definitions'];
			$reportDefinitionUri = array_pop($reportDefinitions);

			$response = $this->restApi->executeReportRaw($bucketAttributes['gd']['pid'], $reportDefinitionUri);
			$csvUri = $response['uri'];

			/** @TODO Streamed import to SAPI
			$stream = $this->restApi->getStream($csvUri);
			$this->uploadToS3($stream->getStream());
			 */

			$filename = $this->mainConfig['tmp_path'] . '/' . uniqid("report-export", true) .'.csv';

			$this->restApi->getToFile($csvUri, $filename);

			$this->uploadToSapi($filename, $params['table'], $params['token']);


			return $this->_prepareResult($job['id'], array(
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());

		} catch (UnauthorizedException $e) {
			throw new WrongConfigurationException('Rest API Login failed');
		} catch (RestApiException $e) {
			return $this->_prepareResult($job['id'], array(
				'status' => 'error',
				'error' => $e->getMessage(),
				'gdWriteStartTime' => $gdWriteStartTime
			), $this->restApi->callsLog());
		}

	}

	protected function uploadToSapi($filename, $tableId, $token)
	{
		$normalizedCsv = $this->normalizeCsv($filename);

		$csvFile = new CsvFile($normalizedCsv);

		$sapi = new \Keboola\StorageApi\Client($token, null, 'gooddata-writer');

		list($stage, $bucket, $tableName) = explode('.', $tableId);
		try {
			$sapi->createTableAsync($stage . '.'. $bucket, $tableName, $csvFile);
		} catch (\Exception $e) {
			$sapi->writeTableAsync($tableId, $csvFile);
		}
	}

	protected function normalizeCsv($filePath)
	{
		$fr = fopen($filePath, 'r');

		$destPath = str_replace('.csv', '-normalized.csv', $filePath);

		$fw = fopen($destPath, 'w');

		$cnt = 0;
		$header = null;

		while ($line = fgets($fr)) {
			if ($cnt == 0) {
				$header = Table::csvStringToArray($line);
				$line = implode(',', Table::normalizeHeader($header[0])) . PHP_EOL;
			}
			fwrite($fw, $line);
			$cnt++;
		}

		fclose($fw);
		fclose($fr);

		return $destPath;
	}

	protected function uploadToS3($stream)
	{
		$client = S3Client::factory(array(
			'key'    => $this->mainConfig['aws']['access_key'],
			'secret' => $this->mainConfig['aws']['secret_key']
		));

		/** @var AbstractTransfer $uploader */
		$uploader = UploadBuilder::newInstance()
			->setClient($client)
			->setSource($stream)
			->setBucket($this->mainConfig['aws']['s3_bucket'])
			->setKey('gooddata-report-export-text.csv')
//			->setOption('Metadata', array('Foo' => 'Bar'))
//			->setOption('CacheControl', 'max-age=3600')
			->build();

		// Perform the upload. Abort the upload if something goes wrong
		try {
			$uploader->upload();
			echo "Upload complete.\n";
		} catch (MultipartUploadException $e) {
			$uploader->abort();
			echo "Upload failed.\n";
		}
	}
}