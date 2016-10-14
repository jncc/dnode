
import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';

import { Hero } from './hero'
import { HeroService } from './hero.service'

@Component({
  selector:    'my-dashboard',
  moduleId:    module.id,
  templateUrl: 'dashboard.component.html',
  styleUrls: [ 'dashboard.component.css' ]
})
export class DashboardComponent implements OnInit {

  constructor(
    private router: Router,
    private heroService: HeroService) {
  }

  heroes: Hero[] = [];
  ngOnInit() {
    this.heroService.getHeroes().then(heroes => this.heroes = heroes.slice(1, 5));
  }

  gotoDetail(hero: Hero) {
    let link = ['/detail', hero.id];
    this.router.navigate(link);
  }
}
