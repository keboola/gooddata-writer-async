<?php
/**
 * @author Jakub Matejka <jakub@keboola.com>
 * @date 2013-03-26
 */
namespace Keboola\GoodDataWriter\Tests\Unit;

use Keboola\GoodDataWriter\Model\Graph;

class GraphTest extends \PHPUnit_Framework_TestCase
{

    public function testAddTransition()
    {
        $graph = new Graph();
        $paths = [];
        $graph->addTransition($paths, 'A', 'B');
        $expected = [
            0 =>
                [
                    'source' => 'A',
                    'target' => 'B',
                    'final' => false,
                    'partial' => false,
                    'path' =>
                        [
                            0 =>
                                [
                                    'tSource' => 'A',
                                    'tTarget' => 'B',
                                ],
                        ],
                ],
        ];
        $this->assertEquals($expected, $paths);
        $graph->addTransition($paths, 'A', 'D');
        $graph->addTransition($paths, 'C', 'D');
        $graph->addTransition($paths, 'A', 'C');

        $expected = [
            0 => [
                'source' => 'A',
                'target' => 'B',
                'final' => false,
                'partial' => false,
                'path' => [
                    0 => [
                        'tSource' => 'A',
                        'tTarget' => 'B',
                    ],
                ],
            ],
            1 => [
                'source' => 'A',
                'target' => 'D',
                'final' => false,
                'partial' => false,
                'path' => [
                    0 => [
                        'tSource' => 'A',
                        'tTarget' => 'D',
                    ],
                ],
            ],
            2 => [
                'source' => 'C',
                'target' => 'D',
                'final' => false,
                'partial' => true,
                'path' => [
                    0 => [
                        'tSource' => 'C',
                        'tTarget' => 'D',
                    ],
                ],
            ],
            3 => [
                'source' => 'A',
                'target' => 'D',
                'final' => false,
                'partial' => false,
                'path' => [
                    0 => [
                        'tSource' => 'C',
                        'tTarget' => 'D',
                    ],
                    1 => [
                        'tSource' => 'A',
                        'tTarget' => 'C',
                    ],
                ],
            ],
        ];
        $this->assertEquals($expected, $paths);
    }


    public function testStartPoints()
    {
        $graph = new Graph();
        $transitions = [
            [
                'tSource' => 'C',
                'tTarget' => 'D',
            ],
            [
                'tSource' => 'A',
                'tTarget' => 'D',
            ],
            [
                'tSource' => 'C',
                'tTarget' => 'D',
            ],
            [
                'tSource' => 'A',
                'tTarget' => 'C',
            ],
        ];
        $this->assertEquals(['A'], $graph->getStartPoints($transitions));
    }

    public function testEndPoints()
    {
        $graph = new Graph();
        $transitions = [
            [
                'tSource' => 'C',
                'tTarget' => 'D',
            ],
            [
                'tSource' => 'A',
                'tTarget' => 'C',
            ],
            [
                'tSource' => 'A',
                'tTarget' => 'D',
            ],
        ];
        $this->assertEquals(['D'], $graph->getEndPoints($transitions));
    }

    public function testTransitivePaths()
    {
        $graph = new Graph();
        $transitions = [
            [
                'tSource' => 'D',
                'tTarget' => 'E',
            ],
            [
                'tSource' => 'A',
                'tTarget' => 'D',
            ],
            [
                'tSource' => 'C',
                'tTarget' => 'D',
            ],
            [
                'tSource' => 'A',
                'tTarget' => 'C',
            ],
            [
                'tSource' => '0',
                'tTarget' => 'A',
            ],
            [
                'tSource' => 'A',
                'tTarget' => 'B',
            ],
        ];

        $transitiveTransitions = $graph->getTransitiveTransitions($transitions);
        $expected = [
            0 => [
                'tSource' => 'A',
                'tTarget' => 'D',
            ],
            1 => [
                'tSource' => 'C',
                'tTarget' => 'D',
            ],
            2 => [
                'tSource' => 'A',
                'tTarget' => 'C',
            ]
        ];
        $this->assertEquals($expected, $transitiveTransitions);
    }

    public function testFindRoutes()
    {
        $graph = new Graph();
        $paths = [];
        $transitions = [
            [
                'tSource' => 'A',
                'tTarget' => 'B',
            ],
            [
                'tSource' => 'A',
                'tTarget' => 'C',
            ],
            [
                'tSource' => 'A',
                'tTarget' => 'D',
            ],
            [
                'tSource' => 'C',
                'tTarget' => 'D',
            ],
        ];
        $graph->findRoutes('B', $transitions, $paths);
        $expected = [
            0 => [
                'source' => 'A',
                'target' => 'B',
                'final' => true,
                'partial' => false,
                'path' => [
                    0 => [
                        'tSource' => 'A',
                        'tTarget' => 'B',
                    ]
                ]
            ]
        ];
        $this->assertEquals($expected, $paths);
        $paths = [];
        $graph->findRoutes('D', $transitions, $paths);
        $expected = [
            0 => [
                'source' => 'A',
                'target' => 'D',
                'final' => true,
                'partial' => false,
                'path' => [
                    0 => [
                        'tSource' => 'A',
                        'tTarget' => 'D',
                    ],
                ],
            ],
            1 => [
                'source' => 'C',
                'target' => 'D',
                'final' => false,
                'partial' => true,
                'path' => [
                    0 => [
                        'tSource' => 'C',
                        'tTarget' => 'D',
                    ],
                ],
            ],
            2 => [
                'source' => 'A',
                'target' => 'D',
                'final' => true,
                'partial' => false,
                'path' => [
                    0 => [
                        'tSource' => 'C',
                        'tTarget' => 'D',
                    ],
                    1 => [
                        'tSource' => 'A',
                        'tTarget' => 'C',
                    ],
                ],
            ],
        ];
        $this->assertEquals($expected, $paths);
    }

    /**
     * @expectedException \Keboola\GoodDataWriter\Exception\GraphTtlException
     */
    public function testFindRoutesTtl()
    {
        $graph = new Graph();
        $paths = [];
        $transitions = [
            [
                'tSource' => 'A',
                'tTarget' => 'B',
            ],
            [
                'tSource' => 'A',
                'tTarget' => 'C',
            ],
            [
                'tSource' => 'A',
                'tTarget' => 'D',
            ],
            [
                'tSource' => 'C',
                'tTarget' => 'D',
            ],
        ];
        $graph->findRoutes('D', $transitions, $paths, 1);
    }
}
