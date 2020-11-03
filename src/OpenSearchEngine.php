<?php

namespace Shengfai\OpenSearch;

use Laravel\Scout\Builder;
use Laravel\Scout\Engines\Engine;
use Illuminate\Config\Repository;
use OpenSearch\Client\OpenSearchClient;
use OpenSearch\Client\DocumentClient;
use OpenSearch\Client\SearchClient;
use OpenSearch\Client\SuggestClient;
use OpenSearch\Util\SearchParamsBuilder;
use OpenSearch\Generated\Common\OpenSearchResult;

/**
 * 开放搜索引擎
 * Class OpenSearchEngine
 *
 * @package \Shengfai\OpenSearch
 * @author ShengFai <shengfai@qq.com>
 */
class OpenSearchEngine extends Engine
{
    /**
     * OpenSearchClient
     *
     * @var \OpenSearch\Client\OpenSearchClient
     */
    protected $client;
    
    /**
     * DocumentClient
     *
     * @var \OpenSearch\Client\DocumentClient
     */
    protected $documentClient;
    
    /**
     * SearchClient
     *
     * @var \OpenSearch\Client\SearchClient
     */
    protected $searchClient;
    
    /**
     * SuggestClient
     *
     * @var \OpenSearch\Client\SuggestClient
     */
    protected $suggestClient;

    public function __construct(Repository $config)
    {
        $accessKey = $config->get('scout.opensearch.accessKey');
        $accessSecret = $config->get('scout.opensearch.accessSecret');
        $host = $config->get('scout.opensearch.host');
        
        $option['debug'] = $config->get('scout.opensearch.debug');
        $option['timeout'] = $config->get('scout.opensearch.timeout');
        
        $this->suggestName = $config->get('scout.opensearch.suggestName');
        $this->client = new OpenSearchClient($accessKey, $accessSecret, $host, $option);
        $this->documentClient = new DocumentClient($this->client);
        $this->searchClient = new SearchClient($this->client);
        $this->suggestClient = new SuggestClient($this->client);
    }

    /**
     *
     * {@inheritDoc}
     *
     * @see \Laravel\Scout\Engines\Engine::update()
     */
    public function update($models)
    {
    
    }

    /**
     *
     * {@inheritDoc}
     *
     * @see \Laravel\Scout\Engines\Engine::delete()
     */
    public function delete($models)
    {
    
    }

    /**
     *
     * @see \Laravel\Scout\Engines\Engine::search()
     */
    public function search(Builder $builder)
    {
        return $this->performSearch($builder, array_filter([
            'numericFilters' => $this->filters($builder),
            'hitsPerPage' => $builder->limit
        ]));
    }

    /**
     *
     * {@inheritDoc}
     *
     * @see \Laravel\Scout\Engines\Engine::paginate()
     */
    public function paginate(Builder $builder, $perPage, $page)
    {
        return $this->performSearch($builder, [
            'numericFilters' => $this->filters($builder),
            'hitsPerPage' => $perPage,
            'page' => $page - 1
        ]);
    }

    /**
     * Perform the given search on the engine.
     *
     * @param \Laravel\Scout\Builder $builder
     * @param array $options
     * @return mixed
     */
    protected function performSearch(Builder $builder, array $options = [])
    {
        // 初始化搜索参数
        $params = new SearchParamsBuilder();
        
        // 分页设置
        $offset = $options['page'] * $options['hitsPerPage'];
        $params->setStart($offset);
        $params->setHits($options['hitsPerPage']);
        
        $params->setAppName($builder->model->searchableAs());
        
        if ($builder->index) {
            $params->setQuery("$builder->index:'$builder->query'");
        } else {
            $params->setQuery("'$builder->query'");
        }
        
        $params->setFormat('fullJson');
        
        // 添加排序字段
        if (count($builder->orders) == 0) {
            $params->addSort('RANK', SearchParamsBuilder::SORT_DECREASE);
        } else {
            foreach ($builder->orders as $value) {
                $params->addSort($value['column'], $value['column'] == 'direction' ? SearchParamsBuilder::SORT_DECREASE : SearchParamsBuilder::SORT_INCREASE);
            }
        }
        
        $results = $this->searchClient->execute($params->build());
        
        return $results;
    }

    /**
     * Get the filter array for the query.
     *
     * @param \Laravel\Scout\Builder $builder
     * @return array
     */
    protected function filters(Builder $builder)
    {
        return collect($builder->wheres)->map(function ($value, $key) {
            return $key . '=' . $value;
        })->values()->all();
    }

    /**
     *
     * {@inheritDoc}
     *
     * @see \Laravel\Scout\Engines\Engine::mapIds()
     */
    public function mapIds($results)
    {
        $data = $this->verifyResults($results);
        return collect($data['result']['items'])->pluck('fields.id')->values();
    }

    /**
     *
     * {@inheritDoc}
     *
     * @see \Laravel\Scout\Engines\Engine::map()
     */
    public function map(Builder $builder, $results, $model)
    {
        $data = $this->verifyResults($results);
        
        if ($data['result']['total'] === 0) {
            return $model->newCollection();
        }
        
        $objectIds = collect($data['result']['items'])->pluck('fields.id')->values()->all();
        $objectIdPositions = array_flip($objectIds);
        
        return $model->getScoutModelsByIds($builder, $objectIds)->filter(function ($model) use($objectIds) {
            return in_array($model->getScoutKey(), $objectIds);
        })->sortBy(function ($model) use($objectIdPositions) {
            return $objectIdPositions[$model->getScoutKey()];
        })->values();
    
    }

    /**
     *
     * {@inheritDoc}
     *
     * @see \Laravel\Scout\Engines\Engine::getTotalCount()
     */
    public function getTotalCount($results)
    {
        $data = $this->verifyResults($results);
        return $data['result']['total'];
    }

    /**
     *
     * {@inheritDoc}
     *
     * @see \Laravel\Scout\Engines\Engine::flush()
     */
    public function flush($model)
    {
    
    }

    /**
     *
     * @param $results
     * @return mixed
     */
    protected function verifyResults($results)
    {
        $result = [];
        if ($results instanceof OpenSearchResult) {
            $result = json_decode($results->result, true);
        }
        
        return $result;
    }
}