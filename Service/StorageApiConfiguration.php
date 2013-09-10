<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-09-09
 */

namespace Keboola\GoodDataWriter\Service;

use Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Table as StorageApiTable;

abstract class StorageApiConfiguration
{

	/**
	 * @var StorageApiClient $_storageApiClient
	 */
	protected $_storageApiClient;

	/**
	 * @param $tableId
	 * @param $whereColumn
	 * @param $whereValue
	 * @param array $options
	 * @return array
	 */
	protected function _fetchTableRows($tableId, $whereColumn = null, $whereValue = null, $options = array())
	{
		$exportOptions = array();
		if ($whereColumn) {
			$exportOptions = array_merge($exportOptions, array(
				'whereColumn' => $whereColumn,
				'whereValues' => array($whereValue)
			));
		}
		if (count($options)) {
			$exportOptions = array_merge($exportOptions, $options);
		}
		$csv = $this->_storageApiClient->exportTable($tableId, null, $exportOptions);
		return StorageApiClient::parseCsv($csv, true);
	}

	/**
	 * @param $tableId
	 * @param $primaryKey
	 * @param $data
	 * @param array $indices
	 * @return \Keboola\StorageApi\Table
	 */
	protected function _updateTableRow($tableId, $primaryKey, $data, $indices = array())
	{
		return $this->_updateTable($tableId, $primaryKey, array_keys($data), array($data), $indices);
	}

	/**
	 * @param $tableId
	 * @param $primaryKey
	 * @param $headers
	 * @param array $indices
	 * @return \Keboola\StorageApi\Table
	 */
	protected function _createTable($tableId, $primaryKey, $headers, $indices = array())
	{
		return $this->_saveTable($tableId, $primaryKey, $headers, array(), false, false, $indices);
	}

	/**
	 * @param $tableId
	 * @param $primaryKey
	 * @param $headers
	 * @param array $data
	 * @param array $indices
	 * @return \Keboola\StorageApi\Table
	 */
	protected function _updateTable($tableId, $primaryKey, $headers, $data = array(), $indices = array())
	{
		return $this->_saveTable($tableId, $primaryKey, $headers, $data, true, true, $indices);
	}

	/**
	 * @param $tableId
	 * @param $primaryKey
	 * @param $headers
	 * @param array $data
	 * @param bool $incremental
	 * @param bool $partial
	 * @param array $indices
	 * @return \Keboola\StorageApi\Table
	 */
	protected function _saveTable($tableId, $primaryKey, $headers, $data = array(), $incremental = true, $partial = true, $indices = array())
	{
		$table = new StorageApiTable($this->_storageApiClient, $tableId, null, $primaryKey);
		$table->setHeader($headers);
		if (count($data)) {
			$table->setFromArray($data);
		}
		if (count($indices)) {
			$table->setIndices($indices);
		}
		$table->setIncremental($incremental);
		$table->setPartial($partial);
		$table->save();
		return $table;
	}
}