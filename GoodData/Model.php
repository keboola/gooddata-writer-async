<?php
/**
 * Created by PhpStorm.
 * User: JakubM
 * Date: 30.01.14
 * Time: 11:03
 */

namespace Keboola\GoodDataWriter\GoodData;

class Model
{

    const TIME_DIMENSION_MANIFEST = 'time-dimension-manifest.json';
    const API_TIME_ZONE = 'Europe/Prague';

    const PROJECT_NAME_TEMPLATE = '%s - %s - %s';
    const PROJECT_NAME_PREFIX = 'KBC';
    const USERNAME_TEMPLATE = '%s-%s@%s';
    const USERNAME_DOMAIN = 'clients.keboola.com';
    const SSO_PROVIDER = 'keboola.com';

    /**
     * Create identifier from name
     */
    public static function getId($name)
    {
        $string = iconv('utf-8', 'ascii//ignore//translit', $name);
        $string = preg_replace('/[^\w\d_]/', '', $string);
        $string = preg_replace('/^[\d_]*/', '', $string);
        return strtolower($string);
    }

    public static function getDateDimensionId($name, $template = null)
    {
        return self::getId($name) . ($template? '.' . $template : null) . '.dataset.dt';
    }

    public static function getTimeDimensionId($name)
    {
        return 'dataset.time.' . self::getId($name);
    }

    public static function getDatasetId($name)
    {
        return 'dataset.' . self::getId($name);
    }

    public static function getAttributeId($tableName, $attrName)
    {
        return sprintf('attr.%s.%s', self::getId($tableName), self::getId($attrName));
    }

    public static function getFactId($tableName, $attrName)
    {
        return sprintf('fact.%s.%s', self::getId($tableName), self::getId($attrName));
    }

    public static function getLabelId($tableName, $attrName)
    {
        return sprintf('label.%s.%s', self::getId($tableName), self::getId($attrName));
    }

    public static function getRefLabelId($tableName, $refName, $attrName)
    {
        return sprintf('label.%s.%s.%s', self::getId($tableName), self::getId($refName), self::getId($attrName));
    }

    public static function getDateFactId($tableName, $attrName)
    {
        return sprintf('dt.%s.%s', self::getId($tableName), self::getId($attrName));
    }

    public static function getTimeFactId($tableName, $attrName)
    {
        return sprintf('tm.dt.%s.%s', self::getId($tableName), self::getId($attrName));
    }

    public static function getTimestampFromApiDate($date)
    {
        $date = new \DateTime($date, new \DateTimeZone(self::API_TIME_ZONE));
        return $date->getTimestamp();
    }

    public function getImplicitConnectionPointId($tableName)
    {
        return sprintf('attr.%s.factsof', self::getId($tableName));
    }


    /**
      * Create json for LDM model manipulation
     */
    public static function getLDM($tableDefinition, $noDateFacts = false)
    {
        // add default connection point
        $dataSet = [
            'identifier' => $tableDefinition['identifier'],
            'title' => $tableDefinition['title'],
            'anchor' => [
                'attribute' => [
                    'identifier' => self::getImplicitConnectionPointId($tableDefinition['tableId']),
                    'title' => sprintf('Records of %s', $tableDefinition['title'])
                ]
            ]
        ];

        $facts = [];
        $attributes = [];
        $references = [];
        $labels = [];
        $connectionPoint = null;
        foreach ($tableDefinition['columns'] as $column) {
            switch($column['type']) {
                case 'CONNECTION_POINT':
                    $connectionPoint = $column['name'];
                    $dataSet['anchor'] = [
                        'attribute' => [
                            'identifier' => !empty($column['identifier']) ? $column['identifier'] : self::getAttributeId($tableDefinition['title'], $column['name']),
                            'title' => $column['title'],
                            'folder' => $tableDefinition['title']
                        ]
                    ];

                    $label = [
                        'identifier' => self::getLabelId($tableDefinition['title'], $column['name']),
                        'title' => $column['title'],
                        'type' => 'GDC.' . ($column['type'] == 'HYPERLINK' ? 'link' : 'text')
                    ];

                    if (!empty($column['dataType'])) {
                        $label['dataType'] = $column['dataType'];
                        if (!empty($column['dataTypeSize'])) {
                            $label['dataType'] .= '(' . $column['dataTypeSize'] . ')';
                        }
                    }

                    $labels[$column['name']][] = [
                        'label' => $label
                    ];
                    break;
                case 'FACT':
                    $fact = [
                        'fact' => [
                            'identifier' => !empty($column['identifier']) ? $column['identifier'] : self::getFactId($tableDefinition['title'], $column['name']),
                            'title' => $column['title'],
                        ]
                    ];
                    if (!empty($column['dataType'])) {
                        $fact['fact']['dataType'] = $column['dataType'];
                        if (!empty($column['dataTypeSize'])) {
                            $fact['fact']['dataType'] .= '(' . $column['dataTypeSize'] . ')';
                        }
                    }
                    $facts[] = $fact;
                    break;
                case 'ATTRIBUTE':
                    $defaultLabelId = self::getLabelId($tableDefinition['title'], $column['name']);
                    $attribute = [
                        'identifier' => !empty($column['identifier']) ? $column['identifier'] : self::getAttributeId($tableDefinition['title'], $column['name']),
                        'title' => $column['title'],
                        'defaultLabel' => $defaultLabelId,
                        'folder' => $tableDefinition['title']
                    ];

                    if (!empty($column['sortLabel'])) {
                        $attribute['sortOrder'] = [
                            'attributeSortOrder' => [
                                'label' => self::getLabelId($tableDefinition['title'], $column['name'].'.'.self::getId($column['sortLabel'])),
                                'direction' => (!empty($column['sortOrder']) && $column['sortOrder'] == 'DESC') ? 'DESC' : 'ASC'
                            ]
                        ];
                    }
                    $attributes[$column['name']] = ['attribute' => $attribute];

                    $label = [
                        'identifier' => $defaultLabelId,
                        'title' => $column['title'],
                        'type' => 'GDC.' . ($column['type'] == 'HYPERLINK' ? 'link' : 'text')
                    ];
                    if (!empty($column['dataType'])) {
                        $label['dataType'] = $column['dataType'];
                        if (!empty($column['dataTypeSize'])) {
                            $label['dataType'] .= '(' . $column['dataTypeSize'] . ')';
                        }
                    }
                    $labels[$column['name']][] = [
                        'label' => $label
                    ];
                    break;
                case 'HYPERLINK':
                case 'LABEL':
                    if (!isset($labels[$column['reference']])) {
                        $labels[$column['reference']] = [];
                    }
                    $label = [
                        'identifier' => self::getRefLabelId($tableDefinition['title'], $column['reference'], $column['name']),
                        'title' => $column['title'],
                        'type' => 'GDC.' . ($column['type'] == 'HYPERLINK' ? 'link' : 'text')
                    ];
                    if (!empty($column['dataType'])) {
                        $label['dataType'] = $column['dataType'];
                        if (!empty($column['dataTypeSize'])) {
                            $label['dataType'] .= '(' . $column['dataTypeSize'] . ')';
                        }
                    } elseif ($column['type'] == 'HYPERLINK') {
                        $label['dataType'] = 'VARCHAR(255)';
                    }
                    $labels[$column['reference']][] = [
                        'label' => $label
                    ];

                    break;
                case 'REFERENCE':
                    $references[] = self::getDatasetId($column['schemaReference']);
                    break;
                case 'DATE':
                    $references[] = self::getId($column['schemaReference']) . (!empty($column['template']) ? '.' . $column['template'] : null);
                    if (!$noDateFacts) {
                        $facts[] = [
                            'fact' => [
                                'identifier' => self::getDateFactId($tableDefinition['title'], $column['name']),
                                'title' => sprintf('%s Date', $column['title'], $tableDefinition['title']),
                                'dataType' => 'INT'
                            ]
                        ];
                    }
                    if ($column['includeTime']) {
                        $references[] = 'dataset.time.' . self::getId($column['schemaReference']);
                        $facts[] = [
                            'fact' => [
                                'identifier' => self::getTimeFactId($tableDefinition['title'], $column['name']),
                                'title' => sprintf('%s Time', $column['title'], $tableDefinition['title']),
                                'dataType' => 'INT'
                            ]
                        ];
                    }
                    break;
            }
        }

        foreach ($labels as $attributeId => $labelArray) {
            if (isset($attributes[$attributeId])) {
                $attributes[$attributeId]['attribute']['labels'] = $labelArray;
            } else if ($attributeId == $connectionPoint) {
                $dataSet['anchor']['attribute']['labels'] = $labelArray;
            }
        }

        if (count($facts)) {
            $dataSet['facts'] = $facts;
        }
        if (count($attributes)) {
            $dataSet['attributes'] = \array_values($attributes);
        }
        if (count($references)) {
            $dataSet['references'] = $references;
        }

        return $dataSet;
    }


    /**
     * Create manifest for data load
     */
    public static function getDataLoadManifest($tableId, $definition, $incrementalLoad, $noDateFacts = false)
    {
        $manifest = [
            'dataSetSLIManifest' => [
                'file' => $tableId . '.csv',
                'dataSet' => $definition['identifier'],
                'parts' => []
            ]
        ];
        foreach ($definition['columns'] as $column) {
            switch ($column['type']) {
                case 'CONNECTION_POINT':
                    $manifest['dataSetSLIManifest']['parts'][] = [
                        'columnName' => $column['name'],
                        'populates' => [
                            self::getLabelId($definition['title'], $column['name'])
                        ],
                        'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
                        'referenceKey' => 1
                    ];
                    break;
                case 'FACT':
                    $manifest['dataSetSLIManifest']['parts'][] = [
                        'columnName' => $column['name'],
                        'populates' => [
                            self::getFactId($definition['title'], $column['name'])
                        ],
                        'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
                    ];
                    break;
                case 'ATTRIBUTE':
                    $manifest['dataSetSLIManifest']['parts'][] = [
                        'columnName' => $column['name'],
                        'populates' => [
                            self::getLabelId($definition['title'], $column['name'])
                        ],
                        'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
                        'referenceKey' => 1
                    ];
                    break;
                case 'LABEL':
                case 'HYPERLINK':
                    $manifest['dataSetSLIManifest']['parts'][] = [
                        'columnName' => $column['name'],
                        'populates' => [
                            self::getRefLabelId($definition['title'], $column['reference'], $column['name'])
                        ],
                        'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
                    ];
                    break;
                case 'REFERENCE':
                    $manifest['dataSetSLIManifest']['parts'][] = [
                        'columnName' => $column['name'],
                        'populates' => [
                            sprintf('label.%s.%s', self::getId($column['schemaReference']), self::getId($column['reference']))
                        ],
                        'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
                        'referenceKey' => 1
                    ];
                    break;
                case 'DATE':
                    $dimensionName = self::getId($column['schemaReference']);
                    $manifest['dataSetSLIManifest']['parts'][] = [
                        'columnName' => $column['name'],
                        'populates' => [
                            sprintf('%s.date.mmddyyyy', $dimensionName . (!empty($column['template']) ? '.' . strtolower($column['template']) : null))
                        ],
                        'constraints' => [
                            'date' => (string)$column['format']
                        ],
                        'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
                        'referenceKey' => 1
                    ];
                    if (!$noDateFacts) {
                        $manifest['dataSetSLIManifest']['parts'][] = [
                            'columnName' => $column['name'] . '_dt',
                            'populates' => [
                                self::getDateFactId($definition['title'], $column['name'])
                            ],
                            'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
                        ];
                    }
                    if ($column['includeTime']) {
                        $manifest['dataSetSLIManifest']['parts'][] = [
                            'columnName' => $column['name'] . '_tm',
                            'populates' => [
                                self::getTimeFactId($definition['title'], $column['name'])
                            ],
                            'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL'
                        ];
                        $manifest['dataSetSLIManifest']['parts'][] = [
                            'columnName' => $column['name'] . '_id',
                            'populates' => [
                                sprintf('label.time.second.of.day.%s', $dimensionName)
                            ],
                            'mode' => $incrementalLoad ? 'INCREMENTAL' : 'FULL',
                            'referenceKey' => 1
                        ];
                    }
                    break;
                case 'IGNORE':
                    break;
            }
        }

        return $manifest;
    }

    /**
     * Create manifest for data load of time dimension
     */
    public static function getTimeDimensionDataLoadManifest($scriptsPath, $dimensionName)
    {
        $timeDimensionManifestPath = $scriptsPath . '/' . self::TIME_DIMENSION_MANIFEST;
        if (!file_exists($timeDimensionManifestPath)) {
            throw new \Exception(sprintf("Time dimension manifest file '%s' does not exist", $timeDimensionManifestPath));
        }
        $manifest = file_get_contents($timeDimensionManifestPath);
        $manifest = str_replace('%NAME%', self::getId($dimensionName), $manifest);
        return $manifest;
    }
}
