package com.simondata.trino;

import io.trino.spi.Plugin;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.security.SystemAccessControl;
import io.trino.spi.security.SystemAccessControlFactory;

import java.util.LinkedList;
import java.util.Map;
import java.lang.Iterable;

public class TrinoPlugins implements Plugin {
    private SystemAccessControlFactory systemAccessControlFactory = new SACFactory();
    private java.util.List<SystemAccessControlFactory> systemAccessControlFactories = new LinkedList<>();

    private EventListenerFactory eventListenerFactory = new ELFactory();
    private java.util.List<EventListenerFactory> eventListenerFactories = new LinkedList<>();

    public TrinoPlugins() {
        this.systemAccessControlFactories.add(systemAccessControlFactory);
        this.eventListenerFactories.add(eventListenerFactory);
    }

    public Iterable<SystemAccessControlFactory> getSystemAccessControlFactories() {
        return this.systemAccessControlFactories;
    }

    public Iterable<EventListenerFactory> getEventListenerFactories() {
        return this.eventListenerFactories;
    }

    public class SACFactory implements SystemAccessControlFactory {
        private void init() {
            Plugins.init();
        }

        public String getName() {
            return AuthPlugin.name();
        }

        public SystemAccessControl create(Map<String, String> config) {
            this.init();

            // Set this to your custom TrinoAuth implementation
            TrinoAuth authImpl = new NamespacedAuth("simon");

            return new CustomSystemAccessControl(authImpl);
        }
    }

    public class ELFactory implements EventListenerFactory {
        private void init() {
            Plugins.init();
        }

        public String getName() {
            return EventsPlugin.name();
        }

        public EventListener create(Map<String, String> config) {
            this.init();

            return QueryEvents.instance();
        }
    }
}
