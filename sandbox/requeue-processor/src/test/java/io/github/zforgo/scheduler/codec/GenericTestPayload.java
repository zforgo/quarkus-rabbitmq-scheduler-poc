package io.github.zforgo.scheduler.codec;

import java.util.Objects;

public class GenericTestPayload<T> {

    private final T foo;
    private String name;

    public GenericTestPayload(T foo) {
        this.foo = foo;
    }

    public GenericTestPayload<T> withName(String name) {
        this.name = name;
        return this;
    }

    public T getFoo() {
        return foo;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof GenericTestPayload<?> that)) {
            return false;
        }
        return Objects.equals(foo, that.foo) && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(foo, name);
    }
}
