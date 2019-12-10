package com.github.fevernova.framework.common.structure.map;


import com.google.common.collect.Lists;
import lombok.ToString;

import java.util.*;


@ToString(exclude = "entrySet")
public class ArrayMap<K, V> implements Map<K, V> {


    private final static Object NULL = new Object();

    private Keys<K> keys;

    private Object[] vals;

    private transient Set<Entry<K, V>> entrySet;


    public ArrayMap(Keys<K> keys) {

        if (keys == null || keys.size() == 0) {
            throw new IllegalArgumentException("ArrayMap Init Error : keys is empty .");
        }
        this.keys = keys;
        this.vals = new Object[this.keys.size()];
    }


    @Override
    public int size() {

        return this.vals.length;
    }


    @Override
    public boolean isEmpty() {

        return this.vals.length == 0;
    }


    @Override
    public boolean containsKey(Object key) {

        Integer p = this.keys.getPosByKey((K) key);
        return p != null && this.vals[p] != null;
    }


    @Override
    public boolean containsValue(Object value) {

        for (int i = 0; i < this.vals.length; i++) {
            if (value == null) {
                if (this.vals[i] == NULL) {
                    return true;
                }
            } else if (value.equals(this.vals[i])) {
                return true;
            }
        }
        return false;
    }


    @Override
    public V get(Object key) {

        Integer p = this.keys.getPosByKey((K) key);
        return p == null ? null : (this.vals[p] == NULL ? null : (V) this.vals[p]);
    }


    @Override
    public V put(K key, V value) {

        Integer p = this.keys.getPosByKey(key);
        if (p == null) {
            return null;
        }
        Object v = this.vals[p];
        this.vals[p] = (value == null ? NULL : value);
        return v == NULL ? null : (V) v;
    }


    @Override
    public V remove(Object key) {

        Integer p = this.keys.getPosByKey((K) key);
        if (p == null) {
            return null;
        }
        Object v = this.vals[p];
        this.vals[p] = null;
        return v == NULL ? null : (V) v;
    }


    @Override
    public void putAll(Map<? extends K, ? extends V> m) {

        for (Entry<? extends K, ? extends V> ent : m.entrySet()) {
            put(ent.getKey(), ent.getValue());
        }
    }


    @Override
    public void clear() {

        this.vals = new Object[keys.size()];
    }


    @Override
    public Set<K> keySet() {

        return this.keys.keysSet();
    }


    @Override
    public Collection<V> values() {

        ArrayList<V> list = Lists.newArrayListWithExpectedSize(this.vals.length);
        for (int i = 0; i < this.vals.length; i++) {
            list.add(this.vals[i] == NULL ? null : (V) this.vals[i]);
        }
        return list;
    }


    @Override
    public Set<Entry<K, V>> entrySet() {

        if (this.entrySet == null) {
            this.entrySet = new EntrySet();
        }
        return this.entrySet;
    }


    final class EntrySet extends AbstractSet<Entry<K, V>> {


        @Override
        public Iterator<Entry<K, V>> iterator() {

            return new EntryIterator();
        }


        @Override
        public int size() {

            return ArrayMap.this.size();
        }
    }


    final class EntryIterator implements Iterator<Entry<K, V>> {


        private EntryX entryx;


        public EntryIterator() {

            this.entryx = new EntryX(-1);
        }


        @Override
        public boolean hasNext() {

            return this.entryx.getPos() < vals.length - 1;
        }


        @Override
        public EntryX next() {

            return this.entryx.next();
        }
    }


    final class EntryX implements Entry<K, V> {


        private int pos;


        public EntryX(int p) {

            this.pos = p;
        }


        @Override
        public K getKey() {

            return keys.getKeyByPos(this.pos);
        }


        @Override
        public V getValue() {

            if (vals[this.pos] == NULL) {
                return null;
            }
            return (V) vals[this.pos];
        }


        public EntryX next() {

            this.pos++;
            return this;
        }


        public int getPos() {

            return this.pos;
        }


        @Override
        public Object setValue(Object value) {

            return putP(this.pos, (V) value);
        }
    }


    public V putP(int p, V value) {

        Object v = this.vals[p];
        this.vals[p] = (value == null ? NULL : value);
        return v == NULL ? null : (V) v;
    }

}
