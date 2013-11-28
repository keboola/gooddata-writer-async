<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-09-09
 */

namespace Keboola\GoodDataWriter\Service;

use Keboola\GoodDataWriter\Exception\WrongConfigurationException;
use Keboola\StorageApi\Config\Reader,
	Keboola\StorageApi\Client as StorageApiClient,
	Keboola\StorageApi\Table as StorageApiTable;

abstract class StorageApiConfiguration
{

	/**
	 * @var StorageApiClient $_storageApiClient
	 */
	protected $_storageApiClient;


	/**
	 * Should contain list of configuration tables, eg:
	 * array(
	 *     self::PROJECT_USERS_TABLE_NAME => array(
	 *         'columns' => array('id', 'pid', 'email', 'role', 'action'),
	 *         'primaryKey' => 'id',
	 *         'indices' => array('pid', 'email')
	 *     )
	 * )
	 * @var array
	 */
	protected $_tables;
	protected $_sapiCache = array();


	/**
	 * @var string
	 */
	public $bucketId;
	
	
	public function __construct($storageApiClient)
	{
		$this->_storageApiClient = $storageApiClient;
	}

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
		return $this->sapi_exportTable($tableId, $exportOptions);
	}

	/**
	 * @param $tableId
	 * @param $whereColumn
	 * @param $whereValue
	 */
	protected function _deleteTableRow($tableId, $whereColumn, $whereValue)
	{
		$this->_deleteTableRows($tableId, $whereColumn, array($whereValue));
	}

	/**
	 * @param $tableId
	 * @param $whereColumn
	 * @param array $whereValues
	 */
	protected function _deleteTableRows($tableId, $whereColumn, $whereValues)
	{
		$options = array(
			'whereColumn' => $whereColumn,
			'whereValues' => $whereValues
		);
		$this->_storageApiClient->deleteTableRows($tableId, $options);
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
		if (count($indices) && !$this->_storageApiClient->tableExists($tableId)) {
			$table->setIndices($indices);
		}
		$table->setIncremental($incremental);
		$table->setPartial($partial);
		$table->save();
		return $table;
	}


	protected function _createConfigTable($tableName)
	{
		if (!isset($this->_tables[$tableName])) return false;

		return $this->_createTable(
			$this->bucketId . '.' . $tableName,
			$this->_tables[$tableName]['primaryKey'],
			$this->_tables[$tableName]['columns'],
			$this->_tables[$tableName]['indices']);
	}

	protected function _updateConfigTable($tableName, $data, $incremental = true)
	{
		if (!isset($this->_tables[$tableName])) return false;

		return $this->_saveTable(
			$this->bucketId . '.' . $tableName,
			$this->_tables[$tableName]['primaryKey'],
			$this->_tables[$tableName]['columns'],
			$data,
			$incremental,
			true,
			$this->_tables[$tableName]['indices']
		);
	}

	protected function _updateConfigTableRow($tableName, $data)
	{
		if (!isset($this->_tables[$tableName])) return false;

		return $this->_updateTableRow(
			$this->bucketId . '.' . $tableName,
			$this->_tables[$tableName]['primaryKey'],
			$data,
			$this->_tables[$tableName]['indices']
		);
	}

	protected function _getConfigTableRow($tableName, $id)
	{
		if (!isset($this->_tables[$tableName])) return false;

		$result = $this->_fetchTableRows($this->bucketId . '.' . $tableName, $this->_tables[$tableName]['primaryKey'], $id);
		return count($result) ? current($result) : false;
	}

	protected function _checkConfigTable($tableName, $columns)
	{
		if (!isset($this->_tables[$tableName])) return false;

		if ($columns != $this->_tables[$tableName]['columns']) {
			throw new WrongConfigurationException(sprintf("Table '%s' appears to be wrongly configured. Contains columns: '%s' but should contain columns: '%s'",
				$tableName, implode(',', $columns), implode(',', $this->_tables[$tableName]['columns'])));
		}

		return true;
	}

	/**
	 * @param $tableName
	 * @return array|bool
	 */
	protected function _getConfigTable($tableName)
	{
		if (!isset($this->_tables[$tableName])) return false;

		$tableId = $this->bucketId . '.' . $tableName;
		if (!$this->_storageApiClient->tableExists($tableId)) {
			$this->_createConfigTable($tableName);
		}
		$table = $this->_fetchTableRows($tableId);
		if (count($table)) {
			$this->_checkConfigTable($tableName, array_keys(current($table)));
		}

		return $table;
	}


	public function bucketInfo()
	{
		return $this->sapi_bucketInfo($this->bucketId);
	}



	/********************
	 ********************
	 * @section SAPI cache
	 * @TODO move somewhere else (maybe create subclass of SAPI client)
	 ********************/


	public function sapi_listBuckets()
	{
		$cacheKey = 'listBuckets';
		if (!isset($this->_sapiCache[$cacheKey])) {
			$this->_sapiCache[$cacheKey] = $this->_storageApiClient->listBuckets();
		}
		return $this->_sapiCache[$cacheKey];
	}

	public function sapi_bucketExists($bucketId)
	{
		foreach ($this->sapi_listBuckets() as $bucket) {
			if ($bucketId == $bucket['id']) {
				return true;
			}
		}
		return false;
	}

	public function sapi_bucketInfo($bucketId)
	{
		$cacheKey = 'bucketInfo.' . $bucketId;
		//if (!isset($this->_sapiCache[$cacheKey])) {
			Reader::$client = $this->_storageApiClient;
			$this->_sapiCache[$cacheKey] = Reader::read($this->bucketId, null, false);
		//}
		return $this->_sapiCache[$cacheKey];
	}

	public function sapi_listTables($bucketId = null)
	{
		$cacheKey = 'listTables.' . $bucketId;
		//if (!isset($this->_sapiCache[$cacheKey])) {
			$this->_sapiCache[$cacheKey] = $this->_storageApiClient->listTables($bucketId, array('include' => ''));
		//}
		return $this->_sapiCache[$cacheKey];
	}

	public function sapi_getTable($tableId)
	{
		$cacheKey = 'getTable.' . $tableId;
		//if (!isset($this->_sapiCache[$cacheKey])) {
			$this->_sapiCache[$cacheKey] = $this->_storageApiClient->getTable($tableId);
		//}
		return $this->_sapiCache[$cacheKey];
	}

	public function sapi_tableExists($tableId)
	{
		$cacheKey = 'tableExists.' . $tableId;
		//if (!isset($this->_sapiCache[$cacheKey])) {
			$this->_sapiCache[$cacheKey] = $this->_storageApiClient->tableExists($tableId);
		//}
		return $this->_sapiCache[$cacheKey];
	}

	public function sapi_exportTable($tableId, $options = array())
	{
		$cacheKey = 'exportTable.' . $tableId;
		if (count($options)) {
			$keyOptions = $options;
			if (isset($keyOptions['whereValues']) && count($keyOptions['whereValues'])) {
				$keyOptions['whereValues'] = implode('.', $keyOptions['whereValues']);
			}
			$cacheKey .=  '.' . implode(':', array_keys($keyOptions)) . '.' . implode(':', array_values($keyOptions));
		}
		//if (!isset($this->_sapiCache[$cacheKey])) {
			$csv = $this->_storageApiClient->exportTable($tableId, null, $options);
			$this->_sapiCache[$cacheKey] = StorageApiClient::parseCsv($csv, true);
		//}
		return $this->_sapiCache[$cacheKey];
	}
}