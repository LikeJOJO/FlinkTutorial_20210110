package com.apple.playground;

public class Girl {
    public String name;
    public int age;
    public String addr;

    public Girl() {

    }

    public Girl(String name, int age, String addr) {
        this.name = name;
        this.age = age;
        this.addr = addr;
    }

    @Override
    public String toString() {
        return "Girl{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", addr='" + addr + '\'' +
                '}';
    }
}
