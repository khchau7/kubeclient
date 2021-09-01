module Kubeclient
  # caches results for multiple consumers to share and keeps them updated with a watch
  class Informer
    def initialize(client, resource_name, reconcile_timeout: 15 * 60, logger: nil, limit: nil)
      @client = client
      @resource_name = resource_name
      @reconcile_timeout = reconcile_timeout
      @logger = logger
      @cache = {}
      @started = nil
      @watching = []
      @limit = limit
    end

    def list
      @cache.values
    end

    def watch(&block)
      with_watching(&block)
    end

    # not implicit so users know they have to `stop`
    def start_worker
      @worker = Thread.new do
        loop do
          fill_cache
          watch_to_update_cache
        rescue StandardError => e
          # need to keep retrying since we work in the background
          @logger&.error("ignoring error during background work #{e}")
        ensure
          sleep(1) # do not overwhelm the api-server if we are somehow broken
        end
      end
      sleep(0.01) until @cache
    end

    def stop_worker
      @worker&.kill # TODO: be nicer ?
    end

    private

    def with_watching
      queue = Queue.new
      @watching << queue
      loop do
        x = queue.pop
        yield(x)
      end
    ensure
      @watching.delete(queue)
    end

    def cache_key(resource)
      resource["metadata"]["uid"]
    end

    def fill_cache
      reply = nil
      continuationToken = nil
      reply = @client.get_entities(nil, @resource_name, limit: @limit)
      if reply.nil? || reply.empty?
        @logger&.error("received reply as nil or empty")
      elsif reply["items"].nil? || reply["items"].empty?
        @logger&.error("received reply items as nil or empty for get_entities")
      else
        reply["items"].each_with_object({}) do |item|
          @cache[cache_key(item)] = getOptimizedItem(item)
        end
        @started = reply["metadata"]["resourceVersion"]
        @logger&.info("resourceVersion: #{@started}")
        continuationToken = reply["metadata"]["continue"]
        while (!continuationToken.nil? && !continuationToken.empty?)
          reply = @client.get_entities(nil, @resource_name, limit: @limit, continue: continuationToken)
          if reply.nil? || reply.empty?
          elsif reply["items"].nil? || reply["items"].empty?
          else
            continuationToken =  reply["metadata"]["continue"]
            reply["items"].each_with_object({}) do |item|
              @cache[cache_key(item)] = getOptimizedItem(item)
            end
          end
        end
      end
    end

    def watch_to_update_cache
      watcher = @client.watch_entities(@resource_name, watch: true, resource_version: @started)
      stop_reason = 'disconnect'

      # stop watcher without using timeout
      Thread.new do
        sleep(@reconcile_timeout)
        stop_reason = 'reconcile'
        watcher.finish
      end

      watcher.each do |notice|
        case notice["type"]
        when 'ADDED', 'MODIFIED' then
          item = notice["object"]
          @cache[cache_key(item)] = getOptimizedItem(item)
        when 'DELETED' then
          @cache.delete(cache_key(notice["object"]))
        when 'ERROR'
          stop_reason = 'error'
          break
        else
          @logger&.error("Unsupported event type #{notice["type"]}")
        end
        @watching.each { |q| q << notice }
      end
      @logger&.info("watch restarted: #{stop_reason}")
    end

    def getOptimizedItem(resourceItem)
      Item = {}
      if !resourceItem["kind"].nil? && resourceItem["kind"] == "Pod"
        Item["apiVersion"] = resourceItem["apiVersion"]
        Item["kind"] =  resourceItem["kind"]
        Item["metadata"] =  {}
        if !resourceItem["metadata"].nil? && !resourceItem["metadata"].empty?
              Item["metadata"]["annotations"] = {}
              if !resourceItem["metadata"]["annotations"].nil? && !resourceItem["metadata"]["annotations"].empty?
                Item["metadata"]["annotations"] =  resourceItem["metadata"]["annotations"]
              end
              Item["metadata"]["labels"] = {}
              if !resourceItem["metadata"]["labels"].nil? && !resourceItem["metadata"]["labels"].empty?
                Item["metadata"]["labels"] =  resourceItem["metadata"]["labels"]
              end
              Item["metadta"]["ownerReferences"] = {}
              if !resourceItem["metadata"]["ownerReferences"].nil? && !resourceItem["metadata"]["ownerReferences"].empty?
                # TODO - can be further optimized
                Item["metadata"]["ownerReferences"] =  resourceItem["metadata"]["ownerReferences"]
              end
              Item["metadata"]["name"] =  resourceItem["metadata"]["name"]
              Item["metadata"]["namespace"] =  resourceItem["metadata"]["namespace"]
              Item["metadata"]["resourceVersion"] =  resourceItem["metadata"]["resourceVersion"]
              Item["metadata"]["uid"] =  resourceItem["metadata"]["uid"]
              Item["metadata"]["creationTimestamp"] =  resourceItem["metadata"]["creationTimestamp"]
        end
        Item["spec"] =  {}
        if !resourceItem["spec"].nil? && !resourceItem["spec"].empty?
          Item["spec"]["containers"] =  []
          if  !resourceItem["spec"]["containers"].nil? && !resourceItem["spec"]["containers"].empty?
            resourceItem["spec"]["containers"].each do | container|
              currentContainer = {}
              currentContainer["name"] = container["name"]
              currentContainer["resources"] = container["resources"]
              Item["spec"]["containers"].push(currentContainer)
            end
          end
          Item["spec"]["nodeName"] = ""
          if !resourceItem["spec"]["nodeName"].nil? && !resourceItem["spec"]["nodeName"].empty?
            Item["spec"]["nodeName"] = resourceItem["spec"]["nodeName"]
          end
        end
        Item["status"] =  {}
        if !resourceItem["status"].nil? && !resourceItem["status"].empty?
          # TODO - can be further optimized
          Item["status"] =  resourceItem["status"]
        end

       return Item
      end
      return resourceItem
    end
  end
end
