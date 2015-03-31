<?php
/**
 * @package gooddata-writer
 * @copyright 2015 Keboola
 * @author Jakub Matejka <jakub@keboola.com>
 */
namespace Keboola\GoodDataWriter\StorageApi;

use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Table;
use Keboola\Syrup\Exception\UserException;

class CachedClient
{
    /**
     * @var Client $client
     */
    protected $client;

    protected $cache = [];


    public function __construct($client)
    {
        $this->client = $client;
    }

    public function clearCache()
    {
        $this->cache = [];
    }

    public function getBucket($bucketId)
    {
        $cacheKey = 'getBucket'.$bucketId;
        if (!isset($this->cache[$cacheKey])) {
            $this->cache[$cacheKey] = $this->client->getBucket($bucketId);
        }
        return $this->cache[$cacheKey];
    }

    public function listBuckets()
    {
        $cacheKey = 'listBuckets';
        if (!isset($this->cache[$cacheKey])) {
            $this->cache[$cacheKey] = $this->client->listBuckets();
        }
        return $this->cache[$cacheKey];
    }

    public function bucketExists($bucketId)
    {
        foreach ($this->listBuckets() as $bucket) {
            if ($bucketId == $bucket['id']) {
                return true;
            }
        }
        return false;
    }

    public function listTables($bucketId = null)
    {
        $cacheKey = 'listTables.' . $bucketId;
        if (!isset($this->cache[$cacheKey])) {
            $this->cache[$cacheKey] = $this->client->listTables($bucketId, ['include' => '']);
        }
        return $this->cache[$cacheKey];
    }

    public function tableExists($tableId)
    {
        foreach ($this->listTables() as $table) {
            if ($tableId == $table['id']) {
                return true;
            }
        }
        return false;
    }

    /**
     * Delete rows from SAPI table
     */
    public function deleteTableRows($tableId, $whereColumn, $whereValues)
    {
        $options = [
            'whereColumn' => $whereColumn,
            'whereValues' => is_array($whereValues) ? $whereValues : [$whereValues]
        ];
        try {
            $this->client->deleteTableRows($tableId, $options);
            $this->clearCache();
        } catch (ClientException $e) {
            switch ($e->getCode()) {
                case 400:
                case 404:
                    // Ignore if table does not exist
                    break;
                case 403:
                    throw new UserException('Your token does not have access to table ' . $tableId);
                    break;
                default:
                    throw $e;
            }
        }
    }

    public function getTable($tableId)
    {
        $cacheKey = 'getTable.' . $tableId;
        if (!isset($this->cache[$cacheKey])) {
            try {
                $this->cache[$cacheKey] = $this->client->getTable($tableId);
            } catch (ClientException $e) {
                if ($e->getCode() == 403) {
                    throw new UserException('Your token does not have access to table ' . $tableId);
                }
                throw $e;
            }
        }
        return $this->cache[$cacheKey];
    }

    public function exportTable($tableId, $whereColumn = null, $whereValue = null, $cache = true)
    {
        $cacheKey = 'exportTable.' . $tableId;
        if ($whereColumn) {
            $options = [
                'whereColumn' => $whereColumn,
                'whereValues' => is_array($whereValue) ? $whereValue : [$whereValue]
            ];
            $cacheKey .= '.'.$whereColumn.'.'.implode('.', $options['whereValues']);
        } else {
            $options = [];
        }

        if (!isset($this->cache[$cacheKey]) || !$cache) {
            try {
                $csv = $this->client->exportTable($tableId, null, $options);
            } catch (ClientException $e) {
                switch ($e->getCode()) {
                    case 403:
                        throw new UserException('Your token does not have access to table ' . $tableId);
                        break;
                    case 404:
                        return [];
                        break;
                    default:
                        throw $e;
                }
            }
            $this->cache[$cacheKey] = Client::parseCsv($csv, true);
        }
        return $this->cache[$cacheKey];
    }

    public function saveTable(
        $tableId,
        array $header,
        array $data = [],
        $primaryKey = null,
        array $indices = [],
        $incremental = true,
        $partial = true
    ) {
        try {
            $table = new Table($this->client, $tableId, null, $primaryKey);
            $table->setHeader($header);
            if (count($data)) {
                $table->setFromArray($data);
            }
            if (count($indices) && !$this->client->tableExists($tableId)) {
                $table->setIndices($indices);
            }
            $table->setIncremental($incremental);
            $table->setPartial($partial);
            $table->save();

            $this->clearCache();
            return $table;
        } catch (ClientException $e) {
            if ($e->getCode() == 403) {
                throw new UserException('Your token does not have access to table ' . $tableId);
            }
            throw $e;
        }
    }

    public function getClient()
    {
        return $this->client;
    }
}
