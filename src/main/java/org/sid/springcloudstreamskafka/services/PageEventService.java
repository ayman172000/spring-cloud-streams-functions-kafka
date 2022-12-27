package org.sid.springcloudstreamskafka.services;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.sid.springcloudstreamskafka.entities.PageEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

//pour demarer un consumer   start bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic R2
//pour demarer un cosumer et afficher la cle et la valeur selon le type:
//start bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic R4 --property print.key=true --property print.value=true --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
@Service
public class PageEventService {
    //pour que cette methode soit deploye il faut ajouter @Bean
    /*par default spring utilise un topic qui porte le meme nom que le nom de fonction mais pour faire le binding avec un topic specifié vous le faire a travers
    le fichier application.properties de la manierre suivante:
    spring.cloud.stream.bindings.pageEventConsumer-in-0.destination=R1
    R1 cest le nom de topic
    pageEventConsumer c'est le nom de la methode consumer
    in sagit d'un input
    si vous voulez utiliser le topic par default l'addresse sa sera pageEventConsumer-in-0
     */
    @Bean
    public Consumer<PageEvent> pageEventConsumer() {
        return (input)->{
            System.out.println("********************");
            System.out.println(input.toString());
            System.out.println("********************");
        };
    }

    /*ce bean permet d'envoyer une page event chaque second vers le topic si vous avez beseoin de configurer le timmer vous pouvez le faire atravers
    le fichier application.properties avec la manierre suivante:
        spring.cloud.stream.poller.fixed-delay=100
    la meme chose pour le consumer spring cloud utilise par default une topic qui porte le nom pageEventSupplier-out-0 et pour choisir un topic specifiée il faut
    le configuré dans application.properties
        spring.cloud.stream.bindings.pageEventSupplier-out-0.destination=R2
     */
    @Bean
    public Supplier<PageEvent> pageEventSupplier()
    {
        return ()-> new PageEvent(
                Math.random()>0.5?"P1":"P2",
                Math.random()>0.5?"U1":"U2",
                new Date(),
                new Random().nextInt(9000));
    }
    /*notre bien que spring suppose que vous avez utiliser une seul fonction soit Consumer soit suplier soit ... si vous utilisé plus que une il faut l'indiquer dans
    application.properties de la maniere suivante:
        spring.cloud.function.definition=pageEventConsumer;pageEventSupplier
     */

    /*
    ce bean permet de lire a travers un topic et ecrire dans un autre topic
    pour personaliser les topic input et output il faut les configurer de la meme façon dans le fichier applications.properties de la manierre suivante:
        spring.cloud.stream.bindings.pageEventFunction-in-0.destination=R2
        spring.cloud.stream.bindings.pageEventFunction-out-0.destination=R3
    sans oublier de declarer la function:
        spring.cloud.function.definition=pageEventConsumer;pageEventSupplier;pageEventFunction
     */
    @Bean
    public Function<PageEvent,PageEvent> pageEventFunction(){
        return (input)->{
            input.setName("changed name");
            input.setUser("changed user");
            return input;
        };
    }

    /*ce bean permet de lire a partir d'un topic mentionné dans le fichier application.properties et ecrire des statistique dans un autre  topic mentionné dans le fichier application.properties
    comme output avec un delai de commit fixée a 1s atravers l'option:
    spring.cloud.stream.kafka.streams.binder.configuration.commit.interval.ms=1000
     */
    @Bean
    public Function<KStream<String,PageEvent>,KStream<String,Long>> kStreamFunction(){
        return (input)->{
            return input
                    .filter((k,v)->v.getDuration()>100)
                    .map((k,v)->new KeyValue<>(v.getName(),0L))
                    .groupBy((k,v)->k, Grouped.with(Serdes.String(),Serdes.Long()))// cette fonction groupBy retourne un KTable
                    .windowedBy(TimeWindows.of(Duration.ofMillis(5000)))
                    .count(Materialized.as("page-count"))//persister les donnée par default kafka il vas stocker dans store qui porte le nom page count mais vous pouvez les stocker dans une base de donnée
                    .toStream()
                    .map((k,v)-> new KeyValue<>("=>"+k.window().startTime()+k.window().endTime()+k.key(),v));
        };
    }
}
