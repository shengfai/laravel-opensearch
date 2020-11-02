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

/**
 * 开放搜索引擎
 * Class OpenSearchEngine
 *
 * @package \Shengfai\OpenSearch
 * @author ShengFai <shengfai@qq.com>
 */
class OpenSearchEngine extends Engine
{
    protected $config;
    protected $client;
    protected $documentClient;
    protected $searchClient;

    public function __construct(Repository $config)
    {
        $accessKey = $config->get('scout.opensearch.accessKey');
        $accessSecret = $config->get('scout.opensearch.accessSecret');
        $host = $config->get('scout.opensearch.host');
        $option['debug'] = $config->get('scout.opensearch.debug');
        $option['timeout'] = $config->get('scout.opensearch.timeout');
        
        $this->config = $config;
        
        $this->client = new OpenSearchClient($accessKey, $accessSecret, $host);
        $this->documentClient = new DocumentClient($this->client);
        $this->searchClient = new SearchClient($this->client);
        $this->suggestClient = new SuggestClient($this->client);
    }

    public function update($models)
    {
        $this->performDocumentsCommand($models, 'ADD');
    }

    public function delete($models)
    {
        $this->performDocumentsCommand($models, 'DELETE');
    }

    public function search(Builder $builder)
    {
        return $this->performSearch($builder, 0, 20);
    }

    public function paginate(Builder $builder, $perPage, $page)
    {
        return $this->performSearch($builder, ($page - 1) * $perPage, $perPage);
    }

    public function mapIds($results)
    {
        $result = $this->checkResults($results);
        if (array_get($result, 'result.num', 0) === 0) {
            return collect();
        }
        
        return collect(array_get($result, 'result.items'))->pluck('fields.id')->values();
    }

    public function map(Builder $builder, $results, $model)
    {
        $result = $this->checkResults($results);
        
        if (array_get($result, 'result.num', 0) === 0) {
            return collect();
        }
        $keys = collect(array_get($result, 'result.items'))->pluck('fields.id')->values()->all();
        $models = $model->whereIn($model->getQualifiedKeyName(), $keys)->get()->keyBy($model->getKeyName());
        
        return collect(array_get($result, 'result.items'))->map(function ($item) use($model, $models) {
            $key = $item['fields']['id'];
            
            if (isset($models[$key])) {
                return $models[$key];
            }
        })->filter()->values();
    }

    public function getTotalCount($results)
    {
        $result = $this->checkResults($results);
        
        return array_get($result, 'result.total', 0);
    }

    /**
     *
     * @param \Illuminate\Database\Eloquent\Collection $models
     * @param string $cmd
     */
    private function performDocumentsCommand($models, string $cmd)
    {
        if ($models->count() === 0) {
            return;
        }
        $appName = $models->first()->openSearchAppName();
        $tableName = $models->first()->getTable();
        
        $docs = $models->map(function ($model) use($cmd) {
            $fields = $model->toSearchableArray();
            
            if (empty($fields)) {
                return [];
            }
            
            return [
                'cmd' => $cmd,
                'fields' => $fields
            ];
        });
        $json = json_encode($docs);
        $this->documentClient->push($json, $appName, $tableName);
    }

    private function performSearch(Builder $builder, $from, $count)
    {
        $params = new SearchParamsBuilder();
        $params->setStart($from);
        $params->setHits($count);
        $params->setAppName($builder->model->openSearchAppName());
        if ($builder->index) {
            $params->setQuery("$builder->index:'$builder->query'");
        } else {
            $params->setQuery("'$builder->query'");
        }
        $params->setFormat('fullJson');
        $params->addSort($builder->model->sortField(), SearchParamsBuilder::SORT_DECREASE);
        
        return $this->searchClient->execute($params->build());
    }

    private function checkResults($results)
    {
        $result = [];
        if ($results instanceof OpenSearchResult) {
            $result = json_decode($results->result, true);
        }
        
        return $result;
    }

    public function flush($model)
    {
    
    }

}