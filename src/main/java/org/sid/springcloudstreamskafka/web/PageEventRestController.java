package org.sid.springcloudstreamskafka.web;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.*;
import org.sid.springcloudstreamskafka.entities.PageEvent;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import javax.print.attribute.standard.Media;
import java.awt.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
@AllArgsConstructor
public class PageEventRestController {
    private InteractiveQueryService interactiveQueryService;
    //cette objet permet de utiliser Stream cloud fonction il fonctionel avec n'importe qu'elle brokers
    // l'injection est faite via le constructeur
    private StreamBridge streamBridge;
    //pour publier un page event sur une page web
    @GetMapping("/publish/{topic}/{name}")
    public PageEvent publish(@PathVariable(name = "topic") String topic,
                             @PathVariable(name = "name") String pageName)
    {
        PageEvent pageEvent=new PageEvent(pageName,Math.random()>0.5?"U1":"U2",new Date(),new Random().nextInt(9000));
        streamBridge.send(topic,pageEvent);
        return pageEvent;
    }

    //pour  cree un consomateur kafka vous pouvez cree une nouvelle application avec les memes dependances mais pour ce que on reste dans la meme application
    /*pour cette raison on peut itiliser l'annotation @messageListinner  qui est definit dans Spring Cloud streams
     mais cette methode est depriciée danc on vas utiliser la programation fonctionel (lambda fct)*/


    //#################################################
    //interagir avec un store kafka
    //principe server send send event
    @GetMapping(value = "/analytics",produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String,Long>> analytics(){
        return Flux.interval(Duration.ofSeconds(1))
                .map(sequence->{
                    Map<String,Long> stringLongMap=new HashMap<>();
                    ReadOnlyWindowStore<String,Long> windowStore=interactiveQueryService.getQueryableStore("page-count", QueryableStoreTypes.windowStore());
                    Instant now=Instant.now();
                    Instant from=now.minusMillis(5000);
                    KeyValueIterator<Windowed<String>,Long> fetchAll=windowStore.fetchAll(from,now);
                    //WindowStoreIterator<Long> fetch=windowStore.fetch(name,from,now); pour recuperer les records d'une page donnée
                    while(fetchAll.hasNext()){
                        KeyValue<Windowed<String>,Long> next=fetchAll.next();
                        stringLongMap.put(next.key.key(), next.value);
                    }
                    return stringLongMap;
                }).share();//share permet de partager les resultat avec tous les users conectés
    }
    //#################################################

}
